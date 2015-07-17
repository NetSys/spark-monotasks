/*
 * Copyright 2014 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.{InterruptibleIterator, Logging, ShuffleDependency, SparkEnv,
  SparkException, TaskContextImpl}
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.network.NetworkMonotask
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BlockManagerId, MonotaskResultBlockId, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator

/**
 * Handles creating network monotasks to read shuffle data over the network and disk monotasks to
 * read shuffle data stored on local disks. Also provides functionality for deserializing,
 * aggregating, and sorting the result.
 *
 * Each task that performs a shuffle should create one instance of this class.  The class stores
 * the intermediate block IDs used to store the shuffle data in the memory store after network
 * monotasks have read it over the network and disk monotasks have read it from local disks.  These
 * IDs are used to later read the data (by the compute monotasks that perform the computation).
 */
class ShuffleHelper[K, V, C](
    shuffleDependency: ShuffleDependency[K, V, C],
    reduceId: Int,
    context: TaskContextImpl)
  extends Logging {

  private val localBlockIds = new ArrayBuffer[BlockId]()
  private val blockManager = SparkEnv.get.blockManager
  private val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()

  // Get the locations of the map output.
  // TODO: Should this fetching of server statuses happen in a network monotask? Could
  //       involve network (to send message to the master); this should be measured.
  private val startTime = System.currentTimeMillis
  private val statuses: Array[(BlockManagerId, Long)] =
    SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleDependency.shuffleId, reduceId)
  logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
    shuffleDependency.shuffleId, reduceId, System.currentTimeMillis - startTime))

  // Store the mapping of block IDs to the map ID in order to properly construct the
  // FetchFailedException if things fail.
  private val blockIdToMapId = new HashMap[BlockId, Int]()

  def getReadMonotasks(): Seq[Monotask] = {
    statuses.zipWithIndex.flatMap { statusAndIndex =>
      val address = statusAndIndex._1._1
      val size = statusAndIndex._1._2
      val mapId = statusAndIndex._2
      // Only need to fetch blocks that have non-zero size.
      if (size > 0) {
        val blockId = new ShuffleBlockId(shuffleDependency.shuffleId, mapId, reduceId)
        blockIdToMapId(blockId) = mapId
        val monotask = if (address.executorId == blockManager.blockManagerId.executorId) {
          // Create a DiskReadMonotask to load the data into memory.
          // The data loaded into memory by these monotasks will be automatically deleted by the
          // LocalDagScheduler, because DiskReadMonotasks always marks the data read from disk as
          // intermediate data that should be deleted when all of the monotasks's dependents
          // complete.
          // TODO: This assumes all shuffle data is stored on-disk; we'll need to update this
          //       when we support reading shuffle data from memory.
          blockManager.getBlockLoadMonotask(blockId, context).getOrElse {
            throw new FetchFailedException(
              blockManager.blockManagerId,
              blockId.shuffleId,
              blockId.mapId,
              reduceId,
              s"Could not find local shuffle block ID $blockId in BlockManager")
          }
        } else {
          new NetworkMonotask(context, address, blockId, size)
        }

        localBlockIds.append(monotask.getResultBlockId())
        Some(monotask)
      } else {
        None
      }
    }
  }

  def getDeserializedAggregatedSortedData(): Iterator[Product2[K, C]] = {
    val shuffleDataSerializer = Serializer.getSerializer(shuffleDependency.serializer)

    // A ShuffleBlockId to use when decompressing shuffle data read from remote executors using the
    // BlockManager. We need to use a ShuffleBlockId for that (as opposed to the
    // MonotaskResultBlockId) so that the block manager correctly determines the compression and
    // settings for the data.
    val dummyShuffleBlockId = new ShuffleBlockId(shuffleDependency.shuffleId, 0, reduceId)

    val iter = localBlockIds.iterator.flatMap { blockId =>
      val dataBuffer = getShuffleDataBuffer(blockId)

      // Can't use BlockManager.dataDeserialize here, because we have an InputStream already
      // (as opposed to a ByteBuffer).
      try {
        val inputStream = dataBuffer.createInputStream()
        val decompressedStream = blockManager.wrapForCompression(dummyShuffleBlockId, inputStream)
        val iter = shuffleDataSerializer.newInstance()
          .deserializeStream(decompressedStream).asIterator
        CompletionIterator[Any, Iterator[Any]](iter, {
          // Once the iterator is exhausted, release the buffer.
          dataBuffer.release()

           // After iterating through all of the shuffle data, aggregate the shuffle read metrics.
           // All calls to updateShuffleReadMetrics() (across all shuffle dependencies) need to
           // happen in a single thread; this is guaranteed here, because this will be called from
           // the task's main compute monotask.
           context.taskMetrics.updateShuffleReadMetrics()
         })
      } catch {
        case e: Exception =>
          logError(s"Failed to get shuffle block $blockId", e)
          val mapId = blockIdToMapId(blockId)
          val address = statuses(mapId)._1
          throw new FetchFailedException(
            address, shuffleDependency.shuffleId, mapId, reduceId, e)
      }
    }

    // Create an iterator that will record the number of records read.
    val recordLoggingIterator = new InterruptibleIterator[Any](context, iter) {
      override def next(): Any = {
        readMetrics.incRecordsRead(1)
        delegate.next()
      }
    }

    getMaybeSortedIterator(getMaybeAggregatedIterator(recordLoggingIterator))
  }

  /**
   * Returns a ManagedBuffer containing the shuffle data corresponding to the given block. Throws
   * an exception if the block could not be found or if the given blockId is not a ShuffleBlockId
   * (for shuffle data read locally) or MonotaskResultBlockId (for shuffle data read from remote
   * executors).
   */
  private def getShuffleDataBuffer(blockId: BlockId): ManagedBuffer = blockId match {
    case shuffleBlockId: ShuffleBlockId =>
      try {
        val bufferMessage = blockManager.getBlockData(shuffleBlockId)
        readMetrics.incLocalBlocksFetched(1)
        readMetrics.incLocalBytesRead(bufferMessage.size)
        bufferMessage
      } catch {
        case e: Exception =>
          val failureMessage = s"Unable to fetch local shuffle block with id $blockId"
          logError(failureMessage, e)
          throw new FetchFailedException(
            blockManager.blockManagerId,
            shuffleBlockId.shuffleId,
            shuffleBlockId.mapId,
            reduceId,
            failureMessage,
            e)
      }

    case monotaskResultBlockId: MonotaskResultBlockId =>
      // TODO: handle case where the block doesn't exist.
      val bufferMessage = blockManager.getSingle(monotaskResultBlockId).get
        .asInstanceOf[ManagedBuffer]
      readMetrics.incRemoteBytesRead(bufferMessage.size)
      readMetrics.incRemoteBlocksFetched(1)
      bufferMessage

    case _ =>
      throw new SparkException(s"Failed to get block $blockId, which is not a shuffle block")
  }

  /** If an aggregator is defined for the shuffle, returns an aggregated iterator. */
  private def getMaybeAggregatedIterator(iterator: Iterator[Any]): Iterator[Product2[K, C]] = {
    if (shuffleDependency.aggregator.isDefined) {
      if (shuffleDependency.mapSideCombine) {
        new InterruptibleIterator(
          context,
          shuffleDependency.aggregator.get.combineCombinersByKey(
            iterator.asInstanceOf[Iterator[_ <: Product2[K, C]]], context))
      } else {
        new InterruptibleIterator(
          context,
          shuffleDependency.aggregator.get.combineValuesByKey(
            iterator.asInstanceOf[Iterator[_ <: Product2[K, V]]], context))
      }
    } else {
      require(!shuffleDependency.mapSideCombine, "Map-side combine without Aggregator specified!")
      iterator.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }
  }

  /** If an ordering is defined, returns a sorted version of the iterator. */
  private def getMaybeSortedIterator(iterator: Iterator[Product2[K, C]]): Iterator[(K, C)] = {
    val sortedIter = shuffleDependency.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Define a Comparator for the whole record based on the key ordering.
        val cmp = new Ordering[Product2[K, C]] {
          override def compare(o1: Product2[K, C], o2: Product2[K, C]): Int = {
            keyOrd.compare(o1._1, o2._1)
          }
        }
        val sortBuffer: Array[Product2[K, C]] = iterator.toArray
        scala.util.Sorting.quickSort(sortBuffer)(cmp)
        sortBuffer.iterator
      case None =>
        iterator
    }
    sortedIter.asInstanceOf[Iterator[(K, C)]]
  }
}

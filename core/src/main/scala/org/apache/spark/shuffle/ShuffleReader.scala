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
  SparkException, TaskContext}
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.network.NetworkMonotask
import org.apache.spark.network.BufferMessage
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockMessage, BlockMessageArray,
  MonotaskResultBlockId, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator

/**
 * Handles creating network monotasks to read shuffle data over the network. Also provides
 * functionality for deserializing, aggregating, and sorting the result.
 *
 * Each task that performs a shuffle should create one instance of this class.  The class stores
 * the intermediate block IDs used to store the shuffle data in the memory store after network
 * monotasks have read it over the network.  These IDs are used to later read the data (by the
 * compute monotasks that perform the computation).
 */
class ShuffleReader[K, V, C](
    shuffleDependency: ShuffleDependency[K, V, C],
    reduceId: Int,
    context: TaskContext)
  extends Logging {

  private val localBlockIds = new ArrayBuffer[BlockId]()

  def getReadMonotasks(): Seq[Monotask] = {
    // Get the locations of the map output.
    // TODO: Should this fetching of server statuses happen in a network monotask? Could
    //       involve network (to send message to the master); this should be measured.
    val startTime = System.currentTimeMillis
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(
      shuffleDependency.shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleDependency.shuffleId, reduceId, System.currentTimeMillis - startTime))

    // Organize the blocks based on the block manager address.
    val blocksByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      val blockId = new ShuffleBlockId(shuffleDependency.shuffleId, index, reduceId)
      blocksByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((blockId, size))
    }

    // Split local and remote blocks.
    val fetchMonotasks = new ArrayBuffer[NetworkMonotask]
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      val nonEmptyBlocks = blockInfos.filter(_._2 != 0)
      if (address == context.env.blockManager.blockManagerId) {
        // Filter out zero-sized blocks
        localBlockIds ++= nonEmptyBlocks.map(_._1)
      } else {
        val networkMonotask = new NetworkMonotask(context, address, nonEmptyBlocks)
        localBlockIds.append(networkMonotask.resultBlockId)
        fetchMonotasks.append(networkMonotask)
      }
    }
    fetchMonotasks
  }

  def getDeserializedAggregatedSortedData(): Iterator[Product2[K, C]] = {
    val shuffleDataSerializer = shuffleDependency.serializer.getOrElse(context.env.serializer)
    val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
    val iter = localBlockIds.iterator.flatMap {
      case shuffleBlockId: ShuffleBlockId =>
        // The map task was run on this machine, so the memory store has the serialized shuffle
        // data.  The block manager transparently handles deserializing the data.
        val blockManager = context.env.blockManager
        blockManager.getLocalBytes(shuffleBlockId) match {
          case Some(bytes) =>
            readMetrics.localBlocksFetched += 1
            blockManager.dataDeserialize(shuffleBlockId, bytes, shuffleDataSerializer)
          case None =>
            logError(s"Unable to fetch local shuffle block with id $shuffleBlockId")
            throw new FetchFailedException(
              blockManager.blockManagerId, shuffleBlockId.shuffleId, shuffleBlockId.mapId, reduceId)
        }

      case monotaskResultBlockId: MonotaskResultBlockId =>
        // TODO: handle case where the block doesn't exist.
        val bufferMessage =
          context.env.blockManager.getSingle(monotaskResultBlockId).get.asInstanceOf[BufferMessage]
        // Remove the data from the memory store.
        // TODO: This should be handled by the LocalDagScheduler, so that it can ensure results
        //       get deleted in all possible failure scenarios.
        //       https://github.com/NetSys/spark-monotasks/issues/8
        context.env.blockManager.removeBlock(monotaskResultBlockId)
        val blockMessageArray = BlockMessageArray.fromBufferMessage(bufferMessage)
        blockMessageArray.flatMap { blockMessage =>
          if (blockMessage.getType != BlockMessage.TYPE_GOT_BLOCK) {
            // TODO: log here where the exception came from (which block manager)
            throw new SparkException("Unexpected message " + blockMessage.getType + " received")
          }
          val blockId = blockMessage.getId
          val networkSize = blockMessage.getData.limit()
          readMetrics.remoteBytesRead += networkSize
          readMetrics.remoteBlocksFetched += 1
          // TODO: is block manager the best place for this deserialization code?
          // This is lazy: it just returns an iterator, but doesn't yet perform the deserialization.
          val deserializedData = context.env.blockManager.dataDeserialize(
            blockId, blockMessage.getData, shuffleDataSerializer)
          deserializedData
        }

      case _ =>
        throw new SparkException("Unexpected type of shuffle ID!")
    }

    /* After iterating through all of the shuffle data, aggregate the shuffle read metrics.
     * All calls to updateShuffleReadMetrics() (across all shuffle dependencies) need to happen
     * in a single thread; this is guaranteed here, because this will be called from the task's
     * main compute monotask. */
    val completionIter = CompletionIterator[Any, Iterator[Any]](iter, {
      context.taskMetrics.updateShuffleReadMetrics()
    })

    getMaybeSortedIterator(getMaybeAggregatedIterator(completionIter))
  }

  /** If an aggregator is defined for the shuffle, returns an aggregated iterator. */
  private def getMaybeAggregatedIterator(iterator: Iterator[Any]): Iterator[Product2[K, C]] = {
    if (shuffleDependency.aggregator.isDefined) {
      if (shuffleDependency.mapSideCombine) {
        new InterruptibleIterator(
          context,
          shuffleDependency.aggregator.get.combineCombinersByKey(
            iterator.asInstanceOf[Iterator[_ <: Product2[K, C]]]))
      } else {
        new InterruptibleIterator(
          context,
          shuffleDependency.aggregator.get.combineValuesByKey(
            iterator.asInstanceOf[Iterator[_ <: Product2[K, V]]]))
      }
    } else if (shuffleDependency.aggregator.isEmpty && shuffleDependency.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
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

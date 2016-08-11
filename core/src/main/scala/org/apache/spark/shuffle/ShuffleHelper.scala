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

import org.apache.spark.{InterruptibleIterator, Logging, ShuffleDependency, SparkEnv,
  TaskContextImpl}
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.network.NetworkRequestMonotask
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
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
    private val shuffleDependency: ShuffleDependency[K, V, C],
    private val reduceId: Int,
    private val context: TaskContextImpl)
  extends Logging {

  private val blockManager = SparkEnv.get.blockManager
  private val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()

  // Get the locations of the map output.
  // TODO: Should this fetching of server statuses happen in a network monotask? Could
  //       involve network (to send message to the master); this should be measured.
  private val startTime = System.currentTimeMillis
  private val statusesByExecutorId: Seq[(BlockManagerId, Seq[(ShuffleBlockId, Long)])] =
    SparkEnv.get.mapOutputTracker.getMapStatusesByExecutorId(shuffleDependency.shuffleId, reduceId)
  logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
    shuffleDependency.shuffleId, reduceId, System.currentTimeMillis - startTime))

  def getReadMonotasks(): Seq[Monotask] = {
    statusesByExecutorId.flatMap {
      case (blockManagerId, blockIdsAndSizes) =>
        val nonZeroBlockIdsAndSizes = blockIdsAndSizes.filter(_._2 > 0)
        if (nonZeroBlockIdsAndSizes.size > 0) {
          getReadMonotasksForBlocks(nonZeroBlockIdsAndSizes, blockManagerId)
        } else {
          None
        }
    }
  }

  /**
   * Returns monotasks that will load the given blocks into memory on this machine, or None if
   * all of the blocks are already in memory.
   */
  private def getReadMonotasksForBlocks(
       blockIdsAndSizes: Seq[(ShuffleBlockId, Long)], location: BlockManagerId): Seq[Monotask] = {
    if (location.executorId == blockManager.blockManagerId.executorId) {
      blockIdsAndSizes.flatMap {
        case (blockId, size) =>
          if (blockManager.memoryStore.contains(blockId)) {
            // If the data is already in local memory, don't need a monotask to load it.
            None
          } else {
            // Create a DiskReadMonotask to load the data into memory.
            // The data loaded into memory by this monotask will be automatically deleted by the
            // LocalDagScheduler, because DiskReadMonotasks always mark the data read from disk as
            // intermediate data that should be deleted when all of the monotask's dependents
            // complete.
            val maybeDiskLoadMonotask = blockManager.getBlockLoadMonotask(blockId, context)
            if (maybeDiskLoadMonotask.isEmpty) {
              throw new FetchFailedException(
                blockManager.blockManagerId,
                blockId.shuffleId,
                blockId.mapId,
                reduceId,
                s"Could not find local shuffle block ID $blockId in BlockManager")
            }
            maybeDiskLoadMonotask
          }
      }
    } else {
      // Need to read the shuffle data from a remote machine. Only read the data that's actually
      // remote -- it's possible much of the data has already opportunistically been fetched early.
      // The NetworkRequest will eventually filter out the local blocks, but filtering here allows
      // us to avoid some network monotasks altogether.
      val filteredShuffleBlockIdsAndSizes = blockIdsAndSizes.filter { blockIdAndSize =>
        !SparkEnv.get.blockManager.memoryStore.contains(blockIdAndSize._1)
      }
      if (filteredShuffleBlockIdsAndSizes.isEmpty) {
        Seq.empty[Monotask]
      } else {
        blockIdsAndSizes.map {
          case (blockId, size) =>
            new NetworkRequestMonotask(context, location, Seq((blockId, size)))
        }
      }
    }
  }

  def getDeserializedAggregatedSortedData(): Iterator[Product2[K, C]] = {
    val shuffleDataSerializer = Serializer.getSerializer(shuffleDependency.serializer)

    val decompressedDeserializedIter = statusesByExecutorId.iterator.flatMap {
      case (blockManagerId, blockIdsAndSizes) =>
        blockIdsAndSizes.flatMap {
          case (blockId, size) =>
            if (size > 0) {
              try {
                getIteratorForBlock(blockId, blockManagerId, shuffleDataSerializer)
              } catch {
                case e: Exception =>
                  logError(s"Failed to get deserialized data for shuffle block $blockId", e)
                  throw new FetchFailedException(
                    blockManagerId, shuffleDependency.shuffleId, blockId.mapId, reduceId, e)
              }
            } else {
              None
            }
        }
    }

    getMaybeSortedIterator(getMaybeAggregatedIterator(decompressedDeserializedIter))
  }

  /**
   * Returns an iterator over the decompressed, deserialized data for the given BlockId. Throws
   * an error if the block could not be found.
   */
  private def getIteratorForBlock(
      blockId: ShuffleBlockId,
      blockManagerId: BlockManagerId,
      serializer: Serializer): Iterator[Any] = {
    val dataBuffer = if (blockManagerId.executorId == blockManager.blockManagerId.executorId) {
      val buffer = blockManager.getBlockData(blockId)
      readMetrics.incLocalBlocksFetched(1)
      readMetrics.incLocalBytesRead(buffer.size)
      buffer
    } else {
      val buffer = blockManager.getSingle(blockId).get.asInstanceOf[ManagedBuffer]
      //readMetrics.incRemoteBlocksFetched(1)
      //readMetrics.incRemoteBytesRead(buffer.size)
      buffer
    }

    // Deserialize and possibly decompress the data.  Can't use BlockManager.dataDeserialize here,
    // because we have an InputStream already (as opposed to a ByteBuffer).
    val inputStream = dataBuffer.createInputStream()
    val decompressedStream = blockManager.wrapForCompression(blockId, inputStream)
    val iter = serializer.newInstance().deserializeStream(decompressedStream).asIterator

    // Create an iterator that will record the number of records read.
    val recordLoggingIterator =
      new InterruptibleIterator[Any](context, iter) {
        override def next(): Any = {
          readMetrics.incRecordsRead(1)
          delegate.next()
        }
      }

    CompletionIterator[Any, Iterator[Any]](recordLoggingIterator, {
      // Once the iterator is exhausted, release the buffer.
      dataBuffer.release()

      // After iterating through all of the shuffle data, aggregate the shuffle read metrics.
      // All calls to updateShuffleReadMetrics() (across all shuffle dependencies) need to
      // happen in a single thread; this is guaranteed here, because this will be called from
      // the task's main compute monotask. This call needs to occur after all metrics updates
      // associated with the shuffle.
      context.taskMetrics.updateShuffleReadMetrics()
    })
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

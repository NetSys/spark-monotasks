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

package org.apache.spark.monotasks.compute

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContextImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.{BlockManagerId, MultipleShuffleBlocksId, ShuffleBlockId}

/**
 * Divides the elements of an RDD into multiple buckets (based on a partitioner specified in the
 * ShuffleDependency) and stores the result in the BlockManager.
 *
 * The parameter `outputSingleBlock` specifies whether the shuffle data should be stored as a
 * single, off-heap buffer (if set to true) or as a set of on-heap buffers, one corresponding to
 * the data for each reduce task (if set to false).
 */
private[spark] class ShuffleMapMonotask[T](
    context: TaskContextImpl,
    rdd: RDD[T],
    private val partition: Partition,
    private val dependency: ShuffleDependency[Any, Any, _],
    private val executorIdToReduceIds: Seq[(BlockManagerId, Seq[Int])],
    private val outputSingleBlock: Boolean)
  extends ExecutionMonotask[T, MapStatus](context, rdd, partition) {

  private val shuffleWriter = SparkEnv.get.shuffleManager.getWriter[Any, Any](
    dependency.shuffleHandle, partition.index, context, outputSingleBlock)

  override def getResult(): MapStatus = {
    val mapStatus = try {
      // First, need to cast the RDD iterator to an iterator of Product2s, because the RDD
      // that's deserialized from the task description doesn't have any type restrictions.
      val castedIterator =
        rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
      // Before writing the iterator, make sure it's an iterator of Tuple2s. This is to maintain
      // consistency with Spark, which always copies the key and value out of shuffled pairs to a
      // new Tuple2 (one reason this is necessary is so that the serialization code path can
      // leverage the more efficient chill serialization, which only works for Tuple2s, and not for
      // MutablePairs, for example, which we might get here if we didn't do the conversion). One
      // example of this code in Spark is here:
      // https://github.com/NetSys/spark-monotasks/blob/spark_with_logging/
      //     core/src/main/scala/org/apache/spark/util/collection/ExternalSorter.scala#L375
      shuffleWriter.write(castedIterator.map(product2 => (product2._1, product2._2)))
      val blockSizes = shuffleWriter.stop(success = true)

      if (SparkEnv.get.conf.getBoolean("spark.monotasks.earlyShuffle", false)) {
        notifyReduceTasksOfMapOutput(blockSizes)
      }

      MapStatus(SparkEnv.get.blockManager.blockManagerId, blockSizes.map(_.toLong))
    } catch {
      case e: Exception =>
        try {
          shuffleWriter.stop(success = false)
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
    mapStatus
  }

  /**
   * Tells the remote machines where reduce tasks are expected to run that the shuffle data is ready
   * to be fetched.
   *
   * TODO: For on-disk data, should decide here whether the data will actually be fetched (since if
   *       it will be fetched, it shouldn't be written to disk).
   */
  private def notifyReduceTasksOfMapOutput(blockSizes: Array[Int]): Unit = {
    val shuffleId = dependency.shuffleHandle.shuffleId
    val blockManagerId = SparkEnv.get.blockManager.blockManagerId
    executorIdToReduceIds.foreach {
      case (remoteBlockManagerId, reduceIds) =>
        if (remoteBlockManagerId != blockManagerId) {
          val reduceIdsWithNonZeroData = reduceIds.filter(blockSizes(_) > 0)
          if (reduceIdsWithNonZeroData.nonEmpty) {
            val shuffleBlockIds = reduceIdsWithNonZeroData.map(
              ShuffleBlockId(shuffleId, partition.index, _).toString)
            val shuffleBlockSizes = reduceIdsWithNonZeroData.map(blockSizes(_))
            // Don't bother scheduling a network monotask for this, since it's just a small RPC.
            logInfo(s"Signalling to $remoteBlockManagerId that shuffle blocks " +
              s"${shuffleBlockIds.mkString(",")} are available")
            SparkEnv.get.blockTransferService.signalBlocksAvailable(
              remoteBlockManagerId.host,
              remoteBlockManagerId.port,
              shuffleBlockIds.toArray,
              shuffleBlockSizes.toArray,
              context.taskAttemptId,
              context.attemptNumber,
              blockManagerId.executorId,
              blockManagerId.host,
              blockManagerId.port)
          } else {
            logDebug(s"Not notifying $remoteBlockManagerId about shuffle data because all " +
              "blocks have zero size")
          }
        }
    }
  }

  /**
   * ShuffleMapMonotask overrides cleanupIntermediateData because it needs to clean up the shuffle
   * data, in addition to the result block containing the serialized task result.
   */
  override def cleanupIntermediateData(): Unit = {
    super.cleanupIntermediateData()
    // Only need to cleanup extra intermediate data if output is stored as a single block, in which
    // case the shuffle data will be written to disk, so the in-memory data is not necessary to
    // keep around.
    if (outputSingleBlock) {
      // Don't tell the master about shuffle block IDs being deleted, because their
      // storage status is tracked by MapStatuses rather than through the BlockManager.
      SparkEnv.get.blockManager.removeBlockFromMemory(
        MultipleShuffleBlocksId(dependency.shuffleId, partition.index), tellMaster = false)
    }
  }
}

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
import org.apache.spark.storage.MultipleShuffleBlocksId

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
    private val outputSingleBlock: Boolean)
  extends ExecutionMonotask[T, MapStatus](context, rdd, partition) {

  private val shuffleWriter = SparkEnv.get.shuffleManager.getWriter[Any, Any](
    dependency.shuffleHandle, partition.index, context, outputSingleBlock)

  override def getResult(): MapStatus = {
    val mapStatus = try {
      shuffleWriter.write(
        rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      shuffleWriter.stop(success = true).get
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

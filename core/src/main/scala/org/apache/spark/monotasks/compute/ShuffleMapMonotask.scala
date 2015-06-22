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

import scala.collection.mutable.HashSet

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContextImpl}
import org.apache.spark.monotasks.disk.DiskWriteMonotask
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.BlockId

/**
 * Divides the elements of an RDD into multiple buckets (based on a partitioner specified in the
 * ShuffleDependency) and stores the result in the BlockManager.
 */
private[spark] class ShuffleMapMonotask[T](
    context: TaskContextImpl,
    rdd: RDD[T],
    partition: Partition,
    dependency: ShuffleDependency[Any, Any, _])
  extends ExecutionMonotask[T, MapStatus](context, rdd, partition) {

  private val shuffleWriter = SparkEnv.get.shuffleManager.getWriter[Any, Any](
    dependency.shuffleHandle, partition.index, context)

  private var resultBlockIds = shuffleWriter.shuffleBlockIds

  def getResultBlockIds(): Seq[BlockId] = resultBlockIds

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
    removeDiskWriteMonotasksForEmptyBlocks(mapStatus)
    mapStatus
  }

    /**
     * Remove any DiskWriteMonotasks that correspond to empty shuffle blocks (these empty blocks
     * will not have been stored in the block manager, since they are empty).
     */
  private def removeDiskWriteMonotasksForEmptyBlocks(mapStatus: MapStatus): Unit = {
    val zeroSizedBlockIds = new HashSet[BlockId]
    resultBlockIds = resultBlockIds.zipWithIndex.flatMap { pair =>
      val blockId = pair._1
      if (mapStatus.getSizeForBlock(pair._2) > 0) {
        Some(blockId)
      } else {
        zeroSizedBlockIds += blockId
        None
      }
    }

    // Eliminate the disk monotasks corresponding to zero-sized blocks from dependents.
    val monotasksToRemove = dependents.filter { dependent =>
      dependent match {
        case diskWriteMonotask: DiskWriteMonotask =>
          zeroSizedBlockIds.contains(diskWriteMonotask.serializedDataBlockId)
        case _ =>
          false
      }
    }
    monotasksToRemove.foreach { monotaskToRemove =>
      monotaskToRemove.dependencies.foreach(_.dependents -= monotaskToRemove)
      monotaskToRemove.dependents.foreach(_.dependencies -= monotaskToRemove)
    }
  }

  /**
   * ShuffleMapMonotask overrides maybeCleanupIntermediate data because it has many result blocks
   * for the shuffle data (in addition to the result block for the serialized task result) that all
   * need to be deleted from the block manager.
   */
  override def cleanupIntermediateData(): Unit = {
    super.cleanupIntermediateData()
    // Don't need to tell the master about shuffle block IDs being deleted, because their
    // storage status is tracked by MapStatuses rather than through the BlockManager.
    resultBlockIds.foreach(SparkEnv.get.blockManager.removeBlockFromMemory(_, tellMaster = false))
  }
}

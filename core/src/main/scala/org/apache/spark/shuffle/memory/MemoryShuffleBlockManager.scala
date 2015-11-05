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

package org.apache.spark.shuffle.memory

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._

import org.apache.spark.{Logging, SparkConf, SparkEnv}
import org.apache.spark.storage.{MultipleShuffleBlocksId, ShuffleBlockId}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}

/**
 * Tracks metadata about the shuffle blocks stored on a particular executor, and periodically
 * deletes old shuffle files.
 */
private[spark] class MemoryShuffleBlockManager(conf: SparkConf) extends Logging {

  type ShuffleId = Int

  private lazy val blockManager = SparkEnv.get.blockManager

  private class ShuffleState(val numBuckets: Int) {
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }
  private val shuffleIdToState = new TimeStampedHashMap[ShuffleId, ShuffleState]()

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  /**
   * Registers shuffle output for a particular map task, so that it can be deleted later by the
   * metadata cleaner.
   */
  def addShuffleOutput(shuffleId: ShuffleId, mapId: Int, numBuckets: Int) {
    shuffleIdToState.putIfAbsent(shuffleId, new ShuffleState(numBuckets))
    shuffleIdToState(shuffleId).completedMapTasks.add(mapId)
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this: shuffleState should be removed only
    // after the corresponding shuffle blocks have been removed.
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleIdToState.remove(shuffleId)
    cleaned
  }

  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleIdToState.get(shuffleId) match {
      case Some(state) =>
        state.completedMapTasks.foreach { mapId =>
          // The shuffle blocks output by mapId will either be stored as a single block identified
          // by a MultipleShuffleBlocksId, or as separate blocks for each reduce task. We remove
          // both potential formats here, since the BlockManager will ignore requests to remove
          // blocks that don't exist.
          blockManager.removeBlock(MultipleShuffleBlocksId(shuffleId, mapId), tellMaster = false)
          (0 until state.numBuckets).foreach { reduceId =>
            blockManager.removeBlock(ShuffleBlockId(shuffleId, mapId, reduceId), tellMaster = false)
          }
        }
        logInfo(s"Deleted all files for shuffle $shuffleId")
        true
      case None =>
        logInfo(s"Could not find files for shuffle $shuffleId for deleting")
        false
    }
  }

  private def cleanup(cleanupTime: Long) {
    shuffleIdToState.clearOldValues(
      cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  def stop() {
    metadataCleaner.cancel()
  }
}

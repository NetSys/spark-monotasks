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

package org.apache.spark.monotasks.disk

import java.io.FileOutputStream

import org.apache.spark.{Logging, TaskContextImpl}
import org.apache.spark.storage.{BlockId, ShuffleBlockId}
import org.apache.spark.util.Utils

/**
 * Fetches the block specified by `serializedDataBlockId` from the memory store and writes it to
 * disk. If the write was successful, updates the BlockManager's metadata about where the block id
 * is stored.
 */
private[spark] class DiskWriteMonotask(
    context: TaskContextImpl,
    blockId: BlockId,
    val serializedDataBlockId: BlockId)
  extends DiskMonotask(context, blockId) with Logging {

  // Identifies the disk on which this DiskWriteMonotask will operate. Set by the DiskScheduler.
  var diskId: Option[String] = None

  /**
   * Attempts to assign this monotask to the given disk.  If the monotask was already assigned to a
   * disk, returns false; otherwise, sets the monotasks disk ID to the given ID and returns true.
   */
  def assignToDisk(potentialDiskId: String): Boolean = synchronized {
    val monotaskNotAssigned = diskId.isEmpty
    if (monotaskNotAssigned) {
      diskId = Some(potentialDiskId)
    }
    monotaskNotAssigned
  }

  override def execute(): Unit = {
    val rawDiskId = diskId.getOrElse(throw new IllegalStateException(
      s"Writing block $blockId to disk failed because the diskId parameter was not set."))
    val blockFileManager = blockManager.blockFileManager

    val data = blockManager.getLocalBytes(serializedDataBlockId)
      .getOrElse(
        throw new IllegalStateException(s"Writing block $blockId to disk $rawDiskId failed " +
          s"because the block's serialized bytes (block $serializedDataBlockId) could not be " +
          "found in memory."))
      .duplicate()

    val file = blockFileManager.getBlockFile(blockId, rawDiskId).getOrElse(
      throw new IllegalStateException(s"Writing block $blockId to disk $rawDiskId failed " +
        "because the BlockFileManager could not provide the appropriate file."))

    val startTimeMillis = System.currentTimeMillis()
    val stream = new FileOutputStream(file)
    val channel = stream.getChannel()
    try {
      channel.write(data)

      blockManager.updateBlockInfoOnWrite(blockId, rawDiskId, data.limit())
      channel.force(true)
    } finally {
      channel.close()
      stream.close()
    }
    logDebug(s"Block ${file.getName()} stored as ${Utils.bytesToString(data.limit())} file " +
      s"(${file.getAbsolutePath()}) on disk $diskId in " +
      s"${System.currentTimeMillis() - startTimeMillis}ms.")

    // Add the block that was written to TaskMetrics.updatedBlocks if it wasn't a shuffle block
    // (the driver doesn't need to know about shuffle blocks written to disk).
    if (!blockId.isInstanceOf[ShuffleBlockId]) {
      recordUpdatedBlocksInTaskMetrics(blockId)
    }
  }

  private def recordUpdatedBlocksInTaskMetrics(blockId: BlockId): Unit = {
    val metrics = context.taskMetrics
    val oldUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq.empty)
    val status = blockManager.getStatus(blockId).getOrElse(
      throw new IllegalStateException(
        s"The BlockManager does not know about block $blockId after it was written to disk."))
    metrics.updatedBlocks = Some(oldUpdatedBlocks ++ Seq((blockId, status)))
  }
}

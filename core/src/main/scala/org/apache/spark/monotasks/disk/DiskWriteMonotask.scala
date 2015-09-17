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
import java.nio.channels.FileChannel

import org.apache.spark.TaskContextImpl
import org.apache.spark.storage.{BlockId, ShuffleBlockId}

/** Handles writing one or more blocks of data to a single disk. */
private[spark] abstract class DiskWriteMonotask(context: TaskContextImpl, blockId: BlockId)
  extends DiskMonotask(context, blockId) {

  // Identifies the disk on which this DiskWriteMonotask will operate. Set by the DiskScheduler.
  var diskId: Option[String] = None

  override def execute(): Unit = {
    val rawDiskId = diskId.getOrElse(throw new IllegalStateException(
      s"Writing block $blockId to disk failed because the diskId parameter was not set."))
    val blockFileManager = blockManager.blockFileManager

    val file = blockFileManager.getBlockFile(blockId, rawDiskId).getOrElse(
      throw new IllegalStateException(s"Writing block $blockId to disk $rawDiskId failed " +
        "because the BlockFileManager could not provide the appropriate file."))

    val stream = new FileOutputStream(file)
    val channel = stream.getChannel()
    try {
      writeData(rawDiskId, channel)
      channel.force(true)
    } finally {
      channel.close()
      stream.close()
    }

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

  /**
   * Writes the data described by this monotask to the given file on the given disk. This method
   * is not responsible for closing the channel or forcing it to disk.
   */
  def writeData(diskId: String, channel: FileChannel): Unit
}

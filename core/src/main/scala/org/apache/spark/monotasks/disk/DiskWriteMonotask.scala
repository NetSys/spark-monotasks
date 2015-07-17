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

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer

import org.apache.spark.{Logging, TaskContextImpl}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

/**
 * Contains the logic and parameters necessary to write a single block to a single disk.
 *
 * A DiskWriteMonotask retrieves the block specified by serializedDataBlockId from the BlockManager
 * and writes it to disk. If the write was successful, the DiskWriteMonotask updates the
 * BlockManager's metadata using the blockId parameter. This separation of functionality between
 * the two provided BlockIds allows a DiskWriteMonotask to fetch the serialized version of a block
 * that was stored in the BlockManager by a SerializationMonotask using a MonotaskResultBlockId and
 * then write that block to disk and update the BlockManager using the block's true BlockId.
 *
 * After the block has been written to disk, the DiskWriteMonotask will delete the block specified
 * by serializedDataBlockId from the MemoryStore.
 */
private[spark] class DiskWriteMonotask(
    context: TaskContextImpl,
    blockId: BlockId,
    val serializedDataBlockId: BlockId)
  extends DiskMonotask(context, blockId) with Logging {

  // Identifies the disk on which this DiskWriteMonotask will operate. Set by the DiskScheduler.
  var diskId: Option[String] = None

  override def execute(): Unit = {
    val rawDiskId = diskId.getOrElse(throw new IllegalStateException(
      s"Writing block $blockId to disk failed because the diskId parameter was not set."))
    val blockFileManager = blockManager.blockFileManager

    val file = blockFileManager.getBlockFile(blockId, rawDiskId).getOrElse(
      throw new IllegalStateException(s"Writing block $blockId to disk $rawDiskId failed " +
        "because the BlockFileManager could not provide the appropriate file."))
    val data = blockManager.getLocalBytes(serializedDataBlockId).getOrElse(
      throw new IllegalStateException(s"Writing block $blockId to disk $rawDiskId failed " +
        s"because the block's serialized bytes (block $serializedDataBlockId) could not be found " +
        "in memory."))
    putBytes(file, data)
    blockManager.updateBlockInfoOnWrite(blockId, rawDiskId, data.limit())

    val metrics = context.taskMetrics
    val oldUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq.empty)
    val updatedBlock = Seq((blockId, blockManager.getStatus(blockId).get))
    metrics.updatedBlocks = Some(oldUpdatedBlocks ++ updatedBlock)
  }

  private def putBytes(file: File, data: ByteBuffer): Unit = {
    logDebug(s"Attempting to write block $blockId to disk $diskId")
    // Copy the ByteBuffer that we are given so that the code that created the ByteBuffer can
    // continue using it. This only copies the metadata, not the buffer contents.
    val dataCopy = data.duplicate()
    val stream = new FileOutputStream(file)
    val channel = stream.getChannel()
    try {
      val startTimeMillis = System.currentTimeMillis()

      while (dataCopy.hasRemaining()) {
        channel.write(dataCopy)
      }
      channel.force(true)

      val endTimeMillis = System.currentTimeMillis()
      logDebug(s"Block ${file.getName()} stored as ${Utils.bytesToString(dataCopy.limit)} file " +
        s"(${file.getAbsolutePath()}) on disk $diskId in ${endTimeMillis - startTimeMillis} ms.")
    } finally {
      channel.close()
      stream.close()
    }
  }
}

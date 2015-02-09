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
import java.nio.ByteBuffer

import scala.util.control.NonFatal

import org.apache.spark.{Logging, TaskContextImpl}
import org.apache.spark.storage.{BlockId, StorageLevel}
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
 * The provided StorageLevel is used to update the BlockManager's metadata if the BlockManager does
 * not know about the block yet.
 *
 * After the block has been written to disk, the DiskWriteMonotask will delete the block specified
 * by serializedDataBlockId from the MemoryStore.
 */
private[spark] class DiskWriteMonotask(
    context: TaskContextImpl,
    blockId: BlockId,
    val serializedDataBlockId: BlockId,
    val level: StorageLevel)
  extends DiskMonotask(context, blockId) with Logging {

  // Identifies the disk on which this DiskWriteMonotask will operate. Set by the DiskScheduler.
  var diskId: Option[String] = None

  override def execute(): Boolean = {
    if (diskId.isEmpty) {
      logError(s"Writing block $blockId to disk failed because of empty diskId parameter.")
      return false
    }
    val rawDiskId = diskId.get
    blockManager.getLocalBytes(serializedDataBlockId).map { data =>
      val success = putBytes(rawDiskId, data)
      if (success) {
        blockManager.updateBlockInfoOnWrite(blockId, level, rawDiskId, data.limit())
        blockManager.removeBlockFromMemory(serializedDataBlockId, false)
      }
      success
    }.getOrElse {
      logError(s"Writing block $blockId to disk failed because the block could not be found.")
      false
    }
  }

  private def putBytes(diskId: String, data: ByteBuffer): Boolean = {
    /* Copy the ByteBuffer that we are given so that the code that created the ByteBuffer can
     * continue using it. This only copies the metadata, not the buffer contents. */
    val dataCopy = data.duplicate()
    logDebug(s"Attempting to write block $blockId to disk $diskId")
    blockManager.blockFileManager.getBlockFile(blockId, diskId).map { file =>
      try {
        val stream = new FileOutputStream(file)
        val channel = stream.getChannel()
        val startTimeMillis = System.currentTimeMillis()
        while (dataCopy.hasRemaining()) {
          channel.write(dataCopy)
        }
        channel.force(true)
        val endTimeMillis = System.currentTimeMillis()
        channel.close()
        stream.close()
        logDebug(s"Block ${file.getName()} stored as ${Utils.bytesToString(dataCopy.limit)} file " +
          s"(${file.getAbsolutePath()}) on disk $diskId in ${endTimeMillis - startTimeMillis} ms.")
        true
      } catch {
        case NonFatal(e) =>
          logError(s"Writing block $blockId to disk $diskId failed due to exception.", e)
          false
      }
    }.getOrElse {
      logError(s"Writing block $blockId to disk $diskId failed " +
        "because the BlockFileManager could not provide the file.")
      false
    }
  }
}

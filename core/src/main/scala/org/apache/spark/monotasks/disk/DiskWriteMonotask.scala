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

import org.apache.spark.Logging
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

/** Contains the parameters and logic necessary to write a block to a single disk. */
private[spark] class DiskWriteMonotask(
    localDagScheduler: LocalDagScheduler,
    blockId: BlockId,
    val data: ByteBuffer)
  extends DiskMonotask(localDagScheduler, blockId) with Logging {

  // Identifies the disk on which this DiskWriteMonotask will operate. Set by the DiskScheduler.
  var diskId: Option[String] = None

  override def execute(): Boolean = {
    if (diskId.isEmpty) {
      logError(s"Writing block $blockId to disk failed because of empty diskId parameter.")
      return false
    }
    val rawDiskId = diskId.get
    val success = putBytes(rawDiskId)
    if (success) {
      blockManager.updateBlockInfoOnWrite(blockId, rawDiskId)
    }
    success
  }

  private def putBytes(diskId: String): Boolean = {
    /* Copy the ByteBuffer that we are given so that the code that created this DiskWriteMonotask
     * can continue using the ByteBuffer that it gave us. This only copies the metadata, not the
     * buffer contents. */
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

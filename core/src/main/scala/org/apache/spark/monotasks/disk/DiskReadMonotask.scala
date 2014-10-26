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

import java.io.FileInputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel.MapMode

import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.{BlockId, MonotaskResultBlockId}

/**
 * Contains the parameters and logic necessary to read a block from disk. The execute() method reads
 * a block from disk and stores the resulting data in the MemoryStore.
 */
private[spark] class DiskReadMonotask(
    localDagScheduler: LocalDagScheduler,
    blockId: BlockId,
    val diskId: String)
  extends DiskMonotask(localDagScheduler, blockId) with Logging {

  private val minMapBytes = blockManager.conf.getLong("spark.storage.memoryMapThreshold", 2 * 4096L)

  override def execute(): Boolean = {
    val data = blockManager.blockFileManager.getBlockFile(blockId, diskId).map {
      file =>
        val stream = new FileInputStream(file)
        val channel = stream.getChannel
        try {
          // For small files, directly read rather than memory map
          if (file.length() < minMapBytes) {
            val buf = ByteBuffer.allocateDirect(file.length().toInt)
            /* Sets dataBuffer's internal byte order (endianness) to the byte order used by the
             * underlying platform. */
            buf.order(ByteOrder.nativeOrder())
            channel.read(buf)
            buf.flip()
          } else {
            channel.map(MapMode.READ_ONLY, 0, file.length())
          }
        } catch {
          case NonFatal(e) =>
            logError(s"Reading block $blockId from disk $diskId failed due to exception.", e)
            None
        } finally {
          channel.close()
          stream.close()
        }
      }
    val success = data.isDefined
    if (success) {
      val resultBlockId = new MonotaskResultBlockId(taskId)
      blockManager.memoryStore.putValue(resultBlockId, data.get).success
    }
    success
  }
}

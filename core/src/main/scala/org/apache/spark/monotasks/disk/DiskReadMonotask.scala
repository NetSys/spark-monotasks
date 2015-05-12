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
import java.nio.ByteBuffer

import org.apache.spark.{Logging, TaskContextImpl}
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * Contains the parameters and logic necessary to read a block from disk. The execute() method reads
 * a block from disk and stores the resulting serialized data in the MemoryStore.
 */
private[spark] class DiskReadMonotask(
    context: TaskContextImpl, blockId: BlockId, val diskId: String)
  extends DiskMonotask(context, blockId) with Logging {

  resultBlockId = Some(blockId)

  override def execute(): Unit = {
    val file = blockManager.blockFileManager.getBlockFile(blockId, diskId).getOrElse(
      throw new IllegalStateException(
        s"Could not read block $blockId from disk $diskId because its file could not be found."))
    val stream = new FileInputStream(file)
    val channel = stream.getChannel
    try {
      val size = file.length().toInt
      val buf = ByteBuffer.allocate(size)

      val startTimeMillis = System.currentTimeMillis()
      channel.read(buf)
      logDebug(s"Block $blockId (size: ${Utils.bytesToString(size)}) read from " +
        s"disk $diskId in ${System.currentTimeMillis - startTimeMillis} ms.")

      buf.flip()
      blockManager.cacheBytes(getResultBlockId(), buf, StorageLevel.MEMORY_ONLY_SER, true)
    } finally {
      channel.close()
      stream.close()
    }
  }
}

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

import java.nio.channels.FileChannel

import org.apache.spark.TaskContextImpl
import org.apache.spark.storage.BlockId

/**
 * Fetches the block specified by `serializedDataBlockId` from the memory store and writes it to
 * disk. If the write was successful, updates the BlockManager's metadata about where the block id
 * is stored.
 */
private[spark] class SingleBlockDiskWriteMonotask(
    context: TaskContextImpl,
    blockId: BlockId,
    val serializedDataBlockId: BlockId)
  extends DiskWriteMonotask(context, blockId) {

  override def writeData(rawDiskId: String, channel: FileChannel) {
    val data = blockManager.getLocalBytes(serializedDataBlockId).getOrElse(
      throw new IllegalStateException(s"Writing block $blockId to disk $rawDiskId failed " +
        s"because the block's serialized bytes (block $serializedDataBlockId) could not be found " +
        "in memory."))
    val dataCopy = data.duplicate()
    channel.write(dataCopy)

    blockManager.updateBlockInfoOnWrite(blockId, rawDiskId, data.limit())
  }
}

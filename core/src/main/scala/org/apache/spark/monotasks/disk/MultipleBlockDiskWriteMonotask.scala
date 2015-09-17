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

import java.io.{BufferedOutputStream, DataOutputStream, FileOutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.TaskContextImpl
import org.apache.spark.storage.{BlockId, ShuffleBlockId, ShuffleIndexBlockId}

/**
 * Handles writing multiple blocks to a single file on disk.  Writes an additional index file that
 * describes the offset of each block within the file.
 *
 * @param context Metadata about the macrotask corresponding to this monotask.
 * @param blockId Id to use for the block stored on disk.
 * @param serializedDataBlockIds Ids of the blocks that should be stored on disk in a single file.
 */
private[spark] class MultipleBlockDiskWriteMonotask(
    context: TaskContextImpl,
    blockId: ShuffleBlockId,
    val serializedDataBlockIds: Seq[BlockId])
  extends DiskWriteMonotask(context, blockId) {

  override def writeData(diskId: String, channel: FileChannel): Unit = {
    // Write each block, in order, to the single output file, tracking how long each block was
    // so that we can write an index file describing where each block starts and ends.
    val lengths = serializedDataBlockIds.map { serializedDataBlockId =>
      val data = blockManager.getLocalBytes(serializedDataBlockId).getOrElse(
        throw new IllegalStateException(s"Writing block $blockId to disk $diskId failed " +
          "because the block's serialized bytes (block $serializedDataBlockId) could not be " +
          "found in memory."))
      val dataCopy = data.duplicate()
      while (dataCopy.hasRemaining()) {
        channel.write(dataCopy)
      }

      val bytesWritten = data.limit()
      // This assumes that the block ID used for the serialized data is the same block ID that
      // will be used later to access the data via the block manager.
      blockManager.updateBlockInfoOnWrite(serializedDataBlockId, diskId, bytesWritten)

      bytesWritten
    }

    // TODO: Consider just writing the indices at the beginning of the data file, rather than
    //       writing a separate file.
    writeIndexFile(blockId.shuffleId, blockId.mapId, lengths, diskId)
  }

  /**
   * Writes an index file that describes the locations of multiple blocks that were all written
   * to the same file. The index file has the offsets of each block, plus a final offset at the end
   * describing the file length.
   *
   * TODO: The functionality to deal with index files is currently spread across the code base;
   *       instead, this functionality should be encapsulated in one class so that other classes
   *       don't need to deal with the added complexity.
   */
  private def writeIndexFile(shuffleId: Int, mapId: Int, lengths: Seq[Int], diskId: String) = {
    val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, 0)
    val indexFile = blockManager.blockFileManager.getFile(indexBlockId.name, diskId).getOrElse(
      throw new IllegalStateException(
        s"Unable to get index file for map $mapId in shuffle $shuffleId")
    )

    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))
    try {
      // For reduce task n, the start position and end position of data for that task are described
      // by the nth and (n+1)th entry in the index file, respectively. A Long is used here (rather
      // than an Int) for consistency with Spark.
      var offset = 0L
      out.writeLong(offset)

      lengths.foreach { length =>
        offset += length
        out.writeLong(offset)
      }
    } finally {
      out.close()
    }
    blockManager.updateBlockInfoOnWrite(
      indexBlockId, diskId, (lengths.length + 1) * 8, tellMaster = false)
  }
}

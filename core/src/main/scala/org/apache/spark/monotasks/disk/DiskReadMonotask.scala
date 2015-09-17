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

import java.io.{DataInputStream, FileInputStream}
import java.nio.ByteBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{Logging, TaskContextImpl}
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.storage.{BlockId, ShuffleBlockId, ShuffleIndexBlockId, StorageLevel}
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
    blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        // Need to read the index file to find the correct location. Since the index file is
        // located on the same disk, we can do this read as part of the same disk monotask.
        val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, 0)
        val indexFile = blockManager.blockFileManager.getBlockFile(indexBlockId, diskId).getOrElse(
          throw new IllegalStateException(
            s"Could not read block $blockId from disk $diskId because the index file " +
            s"($indexBlockId) could not be found")
        )
        val in = new DataInputStream(new FileInputStream(indexFile))
        val (offset, nextOffset) = try {
          ByteStreams.skipFully(in, reduceId * 8)
          (in.readLong(), in.readLong())
        } finally {
          in.close()
        }
        readAndCacheData(ShuffleBlockId(shuffleId, mapId, 0), offset, Some(nextOffset))

      case _ =>
        readAndCacheData(blockId, 0, None)
    }
  }

  /**
   * Reads data from disk and saves the result in-memory using the BlockManager.
   *
   * @param fileBlockId BlockId coresponding to the on-disk file.
   * @param startOffset Offset at which to begin reading.
   * @param endOffset Optional offset at which to end reading. If not specified, this function will
   *                  read to the end of the file.
   */
  private def readAndCacheData(fileBlockId: BlockId, startOffset: Long, endOffset: Option[Long]) = {
    val file = blockManager.blockFileManager.getBlockFile(fileBlockId, diskId).getOrElse(
      throw new IllegalStateException(
        s"Could not read block $blockId from disk $diskId because its file could not be found."))
    val stream = new FileInputStream(file)
    ByteStreams.skipFully(stream, startOffset)
    val channel = stream.getChannel()
    // Assume that the size of the shuffle block is small enough that an Int can describe the
    // length.
    val bytesToRead = (endOffset.getOrElse(file.length()) - startOffset).toInt
    val buffer = ByteBuffer.allocate(bytesToRead)

    try {
      val startTimeMillis = System.currentTimeMillis()
      channel.read(buffer)
      logInfo(s"Block $blockId (size: ${Utils.bytesToString(bytesToRead)}) read from " +
        s"disk $diskId in ${System.currentTimeMillis - startTimeMillis} ms into $buffer")
    } finally {
      channel.close()
      stream.close()
    }

    buffer.flip()
    blockManager.cacheBytes(getResultBlockId(), buffer, StorageLevel.MEMORY_ONLY_SER, true)
    context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Disk).incBytesRead(bytesToRead)
  }
}

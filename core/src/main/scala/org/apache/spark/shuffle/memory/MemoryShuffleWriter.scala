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

package org.apache.spark.shuffle.memory

import java.nio.ByteBuffer

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.storage.{BlockManager, MultipleShuffleBlocksId, ShuffleBlockId,
  StorageLevel}
import org.apache.spark.util.ByteArrayOutputStreamWithZeroCopyByteBuffer

/**
 * A ShuffleWriter that stores all shuffle data in memory using the block manager.
 *
 * The parameter `outputSingleBlock` specifies whether the shuffle data should be stored as a
 * single, off heap buffer (if set to true) or as a set of on-heap buffers, one corresponding to
 * the data for each reduce task (if set to false).
 *
 * If `separateCompression` is true, all of the data will be serialized, and after
 * all of the serialization is finished, all of the data will be compressed.
 * Otherwise, serialization and compression will be pipelined, so a single record
 * is serialized and compressed, then the next record is serialized and compressed, and
 * so on.
 */
private[spark] class MemoryShuffleWriter[K, V](
    shuffleBlockManager: MemoryShuffleBlockManager,
    handle: BaseShuffleHandle[K, V, _],
    private val mapId: Int,
    context: TaskContext,
    private val outputSingleBlock: Boolean,
    separateCompression: Boolean) extends ShuffleWriter[K, V] {

  private val dep = handle.dependency

  // Create a different writer for each output bucket.
  private val blockManager = SparkEnv.get.blockManager
  private val numBuckets = dep.partitioner.numPartitions
  private val shuffleData = Array.tabulate[SerializedObjectWriter](numBuckets) {
    bucketId =>
      new SerializedObjectWriter(blockManager, dep, mapId, bucketId, separateCompression)
  }

  private val shuffleWriteMetrics = new ShuffleWriteMetrics()
  context.taskMetrics().shuffleWriteMetrics = Some(shuffleWriteMetrics)

  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    // Write the data to the appropriate bucket.
    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      shuffleData(bucketId).write(elem)
      shuffleWriteMetrics.incShuffleRecordsWritten(1)
    }
  }

  /**
   * Writes information about where each shuffle block is located within the single file that holds
   * all of the shuffle data.  For shuffle block i, the Int at byte 4*i describes the offset
   * of the first byte of the block, and the Int at byte 4*(i+1) describes the offset of the
   * first byte of the next block.
   */
  private def writeIndexInformation(buffer: ByteBuffer, sizes: Seq[Int], indexSize: Int): Unit = {
    var offset = indexSize
    buffer.putInt(offset)
    sizes.foreach { size =>
      offset += size
      buffer.putInt(offset)
    }
  }

  /**
   * Stops writing shuffle data by storing the shuffle data in the block manager (if the shuffle
   * was successful) and updating the bytes written in the task's ShuffleWriteMetrics.
   */
  override def stop(success: Boolean): Array[Int] = {
    val byteBuffers = shuffleData.map(_.closeAndGetBytes())
    val sizes = byteBuffers.map(_.limit)
    val totalDataSize = sizes.sum
    shuffleWriteMetrics.incShuffleBytesWritten(totalDataSize)

    if (success) {
      if (outputSingleBlock) {
        cacheSingleBlock(byteBuffers, sizes, totalDataSize)
      } else {
        cacheIndividualBlocks(byteBuffers)
      }
      shuffleBlockManager.addShuffleOutput(dep.shuffleId, mapId, numBuckets)
      sizes
    } else {
      Array.empty[Int]
    }
  }

  /** Saves the shuffle data in-memory as a single, off-heap byte buffer. */
  private def cacheSingleBlock(
      shuffleBlocks: Seq[ByteBuffer],
      sizes: Seq[Int],
      totalDataSize: Int): Unit = {
    // Create a new buffer that first has index information, and then has all of the shuffle
    // data. Use a direct byte buffer, so that when a disk monotask writes this data
    // to disk, it spends less time copying the data out of the JVM (for 200MB blocks, using a
    // DirectByteBuffer, as opposed to a regular ByteBuffer, reduced the copying time from about
    // 150ms to about 110ms).
    // TODO: Could wait and write this (relatively small) index data at the beginning of the
    //       disk write monotask, which would allow the disk scheduler to combine multiple
    //       outputs from different map tasks into a single file (or alternately, could just
    //       store the index data in-memory).
    val indexSize = (sizes.length + 1) * 4
    val bufferSize = indexSize + totalDataSize
    val directBuffer = ByteBuffer.allocateDirect(bufferSize)

    writeIndexInformation(directBuffer, sizes, indexSize)

    // Write all of the shuffle data.
    shuffleBlocks.foreach(directBuffer.put(_))

    blockManager.cacheBytes(
      MultipleShuffleBlocksId(dep.shuffleId, mapId),
      directBuffer,
      StorageLevel.MEMORY_ONLY_SER,
      tellMaster = false)
  }

  /** Saves the shuffle data in-memory as a set of individual blocks, one for each reduce task. */
  private def cacheIndividualBlocks(shuffleBlocks: Seq[ByteBuffer]): Unit = {
    shuffleBlocks.zipWithIndex.foreach {
      case (shuffleData, reduceId) =>
        blockManager.cacheBytes(
          ShuffleBlockId(dep.shuffleId, mapId, reduceId),
          shuffleData,
          StorageLevel.MEMORY_ONLY_SER,
          tellMaster = false)
    }
  }
}

/** Serializes and optionally compresses data into an in-memory byte stream. */
private[spark] class SerializedObjectWriter(
    blockManager: BlockManager,
    dep: ShuffleDependency[_,_,_],
    partitionId: Int,
    bucketId: Int,
    private val separateCompression: Boolean) {

  private val byteOutputStream = new ByteArrayOutputStreamWithZeroCopyByteBuffer()
  private val ser = Serializer.getSerializer(dep.serializer.orNull)
  private val shuffleId = dep.shuffleId
  val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)

  /* Only initialize compressionStream and serializationStream if some bytes are written, otherwise
   * 16 bytes will always be written to the byteOutputStream (and those bytes will be unnecessarily
   * transferred to reduce tasks). */
  private var initialized = false
  private var serializationStream: SerializationStream = _

  def open() {
    val maybeCompressionStream = if (separateCompression) {
      byteOutputStream
    } else {
      // Wrap the output stream so that each record is compressed as soon as it's serialized.
      blockManager.wrapForCompression(blockId, byteOutputStream)
    }
    serializationStream = ser.newInstance().serializeStream(maybeCompressionStream)
    initialized = true
  }

  def write(value: Any) {
    if (!initialized) {
      open()
    }
    serializationStream.writeObject(value)
  }

  /**
   * Closes the byte stream and returns a ByteBuffer containing the serialized data.
   */
  def closeAndGetBytes(): ByteBuffer = {
    if (initialized) {
      serializationStream.flush()
      serializationStream.close()
      if (separateCompression) {
        // We didn't compress the data as it was being written to the output stream,
        // so compress it all now.
        val compressedOutputStream = new ByteArrayOutputStreamWithZeroCopyByteBuffer()
        val compressionStream = blockManager.wrapForCompression(blockId, compressedOutputStream)
        compressionStream.write(byteOutputStream.toByteArray)
        compressionStream.flush()
        compressionStream.close()
        return compressedOutputStream.getByteBuffer()
      }
    }
    byteOutputStream.getByteBuffer()
  }
}

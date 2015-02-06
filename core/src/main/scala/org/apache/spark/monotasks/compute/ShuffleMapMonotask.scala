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

package org.apache.spark.monotasks.compute

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.{MapOutputTracker, Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}

/**
 * Divides the elements of an RDD into multiple buckets (based on a partitioner specified in the
 * ShuffleDependency) and stores the result in the BlockManager.
 */
private[spark] class ShuffleMapMonotask[T](
    context: TaskContext,
    rdd: RDD[T],
    partition: Partition,
    dependency: ShuffleDependency[Any, Any, _])
  extends ExecutionMonotask[T, MapStatus](context, rdd, partition) {

  override def getResult(): MapStatus = {
    // Optionally do map-side combining before outputting shuffle files.
    val uncombinedIterator =
      rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
    val iter = if (dependency.aggregator.isDefined && dependency.mapSideCombine) {
      dependency.aggregator.get.combineValuesByKey(uncombinedIterator)
    } else if (dependency.aggregator.isEmpty && dependency.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      uncombinedIterator
    }

    // Create a different writer for each output bucket.
    val blockManager = SparkEnv.get.blockManager
    val shuffleData = Array.tabulate[SerializedObjectWriter](dependency.partitioner.numPartitions) {
      bucketId =>
        new SerializedObjectWriter(blockManager, dependency, partition.index, bucketId)
    }
    for (elem <- iter) {
      val bucketId = dependency.partitioner.getPartition(elem._1)
      shuffleData(bucketId).write(elem)
    }

    // Store the shuffle data in the block manager and get the sizes.
    val shuffleWriteMetrics = new ShuffleWriteMetrics()
    context.taskMetrics.shuffleWriteMetrics = Some(shuffleWriteMetrics)
    val compressedSizes = shuffleData.map { shuffleWriter =>
      val bytesWritten = shuffleWriter.saveToBlockManager()
      shuffleWriteMetrics.shuffleBytesWritten += bytesWritten
      MapOutputTracker.compressSize(bytesWritten)
    }
    context.markTaskCompleted()

    new MapStatus(SparkEnv.get.blockManager.blockManagerId, compressedSizes)
  }
}

private class SerializedObjectWriter(
    blockManager: BlockManager,
    dep: ShuffleDependency[_,_,_],
    partitionId: Int,
    bucketId: Int) {
  private val byteOutputStream = new ByteArrayOutputStream()
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val shuffleId = dep.shuffleId
  private val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)

  // Only initialize compressionStream and serializationStream if some bytes are written, otherwise
  // 16 bytes will always be written to the byteOutputStream (and those bytes will be unnecessarily
  // transferred to reduce tasks).
  private var initialized = false
  private var compressionStream: OutputStream = _
  private var serializationStream: SerializationStream = _

  def open() {
    compressionStream = blockManager.wrapForCompression(blockId, byteOutputStream)
    serializationStream = ser.newInstance().serializeStream(compressionStream)
    initialized = true
  }

  def write(value: Any) {
    if (!initialized) {
      open()
    }
    serializationStream.writeObject(value)
  }

  def saveToBlockManager(): Long = {
    if (initialized) {
      serializationStream.flush()
      serializationStream.close()
      // TODO: toByteArray creates a copy of byteOutputStream; change MemoryStore to store streams
      //       so that we can avoid this copy (ByteBuffer.wrap does not cause a copy).
      //       See http://stackoverflow.com/questions/2716596/
      //         how-to-put-data-from-an-outputstream-into-a-bytebuffer
      //       for a way to do this that involves subclassing ByteArrayOutputStream.
      // TODO: Need to delete this data after the reduce task completes!
      // TODO: Don't tell block manager master about shuffle blocks! It results in many extra
      //       messages.
      blockManager.putBytes(
        blockId, ByteBuffer.wrap(byteOutputStream.toByteArray), StorageLevel.MEMORY_ONLY_SER)
      return byteOutputStream.size()
    } else {
      return 0
    }
  }
}

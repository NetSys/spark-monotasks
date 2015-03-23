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

import java.io.DataOutputStream
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.{Partition, SparkEnv, TaskContextImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{MonotaskResultBlockId, StorageLevel}
import org.apache.spark.util.ByteArrayOutputStreamWithZeroCopyByteBuffer

/**
 * Uses an org.apache.hadoop.mapreduce.RecordWriter to serialize and (maybe) compress the provided
 * RDD. The resulting bytes are cached in the MemoryStore using a MonotaskResultBlockId, where they
 * will be retrieved by an HdfsWriteMonotask.
 */
private[spark] class HdfsSerializationMonotask(
    sparkTaskContext: TaskContextImpl,
    private val rdd: RDD[(_, _)],
    private val sparkPartition: Partition,
    private val hadoopTaskContext: TaskAttemptContext,
    private val hadoopConf: Configuration,
    private val codec: Option[CompressionCodec])
  extends ComputeMonotask(sparkTaskContext) {

  resultBlockId = Some(new MonotaskResultBlockId(taskId))

  /**
   * The number of records that this HdfsSerializationMonotask has serialized. This is used by
   * HdfsWriteMonotask to update the TaskMetrics with how many records were written to disk.
   */
  private var numRecordsSerialized = 0L

  def getNumRecordsSerialized(): Long = numRecordsSerialized

  override def execute(): Option[ByteBuffer] = {
    val byteStream = new ByteArrayOutputStreamWithZeroCopyByteBuffer()
    val outputStream =
      new DataOutputStream(codec.map(_.createOutputStream(byteStream)).getOrElse(byteStream))
    // Default value chosen to be consistent with
    // org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.getRecordWriter()
    val separator = hadoopConf.get("mapreduce.output.textoutputformat.separator", "\t")
    val writer = new TextOutputFormat.LineRecordWriter[Any, Any](outputStream, separator)

    val iterator = rdd.iterator(sparkPartition, context)
    try {
      while (iterator.hasNext) {
        val pair = iterator.next()
        writer.write(pair._1, pair._2)
        numRecordsSerialized += 1
      }
      SparkEnv.get.blockManager.cacheBytes(
        getResultBlockId(), byteStream.getByteBuffer(), StorageLevel.MEMORY_ONLY_SER, false)
    } finally {
      // This also closes byteStream and outputStream.
      writer.close(hadoopTaskContext)
    }

    None
  }
}

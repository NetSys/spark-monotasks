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

import java.io.IOException
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.SparkHadoopMapReduceUtil
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import org.apache.spark.{Logging, Partition, TaskContext}
import org.apache.spark.executor.{DataReadMethod, InputMetrics}
import org.apache.spark.rdd.NewHadoopPartition
import org.apache.spark.storage.{MonotaskResultBlockId, RDDBlockId, StorageLevel}

/**
 * Contains the parameters and logic necessary to read a block from HDFS and cache it in memory.
 * The serialized data that this monotask stores in memory is deleted by the compute() method of
 * the NewHadoopRDD that the data represents.
 */
private[spark] class HdfsReadMonotask(
    context: TaskContext,
    rddId: Int,
    partition: Partition,
    jobTrackerId: String,
    confBroadcast: Configuration)
  extends DiskMonotask(context, new RDDBlockId(rddId, partition.index))
  with SparkHadoopMapReduceUtil
  with Logging {

  private val taskAttemptId = newTaskAttemptID(jobTrackerId, rddId, true, partition.index, 0)
  private val conf = newTaskAttemptContext(confBroadcast, taskAttemptId).getConfiguration()
  private val fileSplit =
    partition.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value match {
      case file: FileSplit =>
        file
      case split: Any =>
        throw new UnsupportedOperationException(s"Unsupported InputSplit: $split. " +
          "HdfsReadMonotask only supports InputSplits of type FileSplit")
  }

  val path = fileSplit.getPath()
  val resultBlockId = new MonotaskResultBlockId(taskId)

  override def execute(): Boolean = {
    val stream = path.getFileSystem(conf).open(path)
    // If this is not the first split, then we also need to read the last byte from the previous
    // split. This is due to the fact that RecordReaders look at the last byte of the previous split
    // in order to determine if they need to drop the first record in their split.
    val start = fileSplit.getStart()
    val bytesFromPreviousSplit = if (start == 0) 0 else 1
    val buffer = new Array[Byte](fileSplit.getLength().toInt + bytesFromPreviousSplit)

    try {
      stream.readFully(start - bytesFromPreviousSplit, buffer)
      context.localDagScheduler.blockManager.cacheBytes(
        resultBlockId, ByteBuffer.wrap(buffer), StorageLevel.MEMORY_ONLY_SER, true)
      true
    } catch {
      case e: IOException => {
        logError(s"Reading block $blockId from HDFS failed due to an exception.", e)
        false
      }
    } finally {
      stream.close()

      val inputMetrics = new InputMetrics(DataReadMethod.Hadoop)
      inputMetrics.bytesRead = buffer.length
      context.taskMetrics.inputMetrics = Some(inputMetrics)
    }
  }
}

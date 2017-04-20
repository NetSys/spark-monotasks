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

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{Partition, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.rdd.NewHadoopPartition
import org.apache.spark.storage.{MonotaskResultBlockId, RDDBlockId, StorageLevel}

/** Contains the parameters and logic necessary to read an HDFS file and cache it in memory. */
private[spark] class HdfsReadMonotask(
    sparkTaskContext: TaskContextImpl,
    rddId: Int,
    sparkPartition: Partition,
    hadoopConf: Configuration)
  extends HdfsDiskMonotask(
    sparkTaskContext,
    new RDDBlockId(rddId, sparkPartition.index),
    hadoopConf) {

  private val hadoopSplit =
    sparkPartition.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value

  pathOpt = Some(hadoopSplit.getPath())
  resultBlockId = Some(new MonotaskResultBlockId(taskId))

  override def execute(): Unit = {
    val path = getPath()
    val stream = path.getFileSystem(hadoopConf).open(path)
    val sizeBytes = hadoopSplit.getLength().toInt
    val buffer = new Array[Byte](sizeBytes)
    val startTime = System.nanoTime()

    try {
      stream.readFully(hadoopSplit.getStart(), buffer)
    } finally {
      stream.close()
    }

    SparkEnv.get.blockManager.cacheBytes(
      getResultBlockId(), ByteBuffer.wrap(buffer), StorageLevel.MEMORY_ONLY_SER, false)
    context.taskMetrics.incDiskReadNanos(System.nanoTime() - startTime)
    context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Hadoop).incBytesRead(sizeBytes)
  }
}

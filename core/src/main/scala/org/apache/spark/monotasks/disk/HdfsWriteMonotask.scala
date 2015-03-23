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

import scala.util.control.NonFatal
import scala.reflect.classTag
import scala.util.Random

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}

import org.apache.spark.{Logging, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.storage.BlockId

/**
 * Contains the logic and parameters necessary to write the block specified by
 * `serializedDataBlockId` (which should be stored in the BlockManager) to the Hadoop Distributed
 * File System.
 *
 * Accepts a parameter `getNumRecords` that is a function that should return the number of records
 * that this HdfsWriteMonotask will write to disk. Since this monotask only deals with serialized
 * bytes, it has no way to determine the number of records for itself. This is used to update the
 * task metrics after the write operation has completed.
 */
private[spark] class HdfsWriteMonotask(
    private val sparkTaskContext: TaskContextImpl,
    blockId: BlockId,
    private val serializedDataBlockId: BlockId,
    private val hadoopConf: Configuration,
    private val hadoopTaskContext: TaskAttemptContext,
    private val codec: Option[CompressionCodec],
    private val getNumRecords: () => Long)
  extends HdfsDiskMonotask(sparkTaskContext, blockId) with Logging {

  override def execute(): Unit = {
    val buffer = SparkEnv.get.blockManager.getLocalBytes(serializedDataBlockId).getOrElse(
      throw new IllegalStateException(
        s"Writing block $blockId to HDFS failed because the block's serialized bytes " +
        s"(stored with blockId $serializedDataBlockId) could not be found."))

    val byteArray = if (buffer.hasArray()) {
      // The ByteBuffer has a backing array that can be accessed without copying the bytes. However,
      // the backing array might be larger than the number of bytes that are stored in the
      // ByteBuffer, so we need to use the ByteBuffer's metadata to select only the relevant portion
      // of the array.
      buffer.array().take(buffer.duplicate().position(0).remaining())
    } else {
      val bufferCopy = buffer.duplicate()
      bufferCopy.rewind()
      val destByteArray = new Array[Byte](bufferCopy.remaining())
      bufferCopy.get(destByteArray)
      destByteArray
    }

    val outputCommitter = constructOutputCommitter()
    val path = constructPath(outputCommitter)

    var isSetUp = false
    var stream: FSDataOutputStream = null
    try {
      outputCommitter.setupTask(hadoopTaskContext)
      isSetUp = true

      stream = path.getFileSystem(hadoopConf).create(path, false)
      stream.write(byteArray)
      stream.hsync()
      logInfo(s"Block $blockId was successfully saved to HDFS at location: ${path.toString()}")
    } catch {
      case NonFatal(e) => {
        if (isSetUp) {
          outputCommitter.abortTask(hadoopTaskContext)
        }
        throw e
      }
    } finally {
      if (stream != null) {
        stream.close()
      }
    }

    SparkHadoopMapRedUtil.commitTask(outputCommitter, hadoopTaskContext, sparkTaskContext)
    val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    sparkTaskContext.taskMetrics.outputMetrics = Some(outputMetrics)
    outputMetrics.setRecordsWritten(getNumRecords())
  }

  /** Returns a FileOutputCommitter for the Hadoop task that this HdfsDiskMonotask represents. */
  private def constructOutputCommitter(): FileOutputCommitter = {
    val format = new Job(hadoopConf).getOutputFormatClass.newInstance()
    format match {
      case configurable: Configurable => configurable.setConf(hadoopConf)
      case _ =>
    }
    format.getOutputCommitter(hadoopTaskContext) match {
      case fileCommitter: FileOutputCommitter =>
        fileCommitter
      case otherCommitter =>
        throw new UnsupportedOperationException("Unsupported OutputCommitter: " +
          s"${otherCommitter.getClass().getName()}. HdfsWriteMonotask only supports " +
          s"OutputCommitters of type ${classTag[FileOutputCommitter]}.")
    }
  }

  /**
   * Uses the provided FileOutputCommitter to construct the path to the file that this
   * HdfsWriteMonotask will create. Constructing the path would normally be handled internally by
   * the FileOutputFormat class. However, we cannot use that class because we need to separate the
   * process of serializing the data from the act of writing it to disk. As a result, we need to
   * assemble the path ourselves.
   *
   * @param outputCommitter The FileOutputCommitter corresponding to the Hadoop task that this
   *                        HdfsWriteMonotask represents.
   */
  private def constructPath(outputCommitter: FileOutputCommitter): Path = {
    val prefix = hadoopConf.get(FileOutputFormat.BASE_OUTPUT_NAME, FileOutputFormat.PART)
    val extension = codec.map(_.getDefaultExtension).getOrElse("")
    val filename = FileOutputFormat.getUniqueFile(hadoopTaskContext, prefix, extension)
    new Path(outputCommitter.getWorkPath(), filename)
  }

  /**
   * TODO: We cannot use the HDFS API to determine which disk a file is stored on (and therefore
   *       which local directory to choose) until after the file has been written. This means that
   *       for HdfsWriteMonotasks, we currently have no way to choose a local directory. We need to
   *       find a way around this. For now, we will just return a random local directory.
   */
  override def chooseLocalDir(sparkLocalDirs: Seq[String]): String =
    sparkLocalDirs(Random.nextInt(sparkLocalDirs.size))
}

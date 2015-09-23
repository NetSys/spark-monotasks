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
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}

import org.apache.spark.{Logging, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.storage.BlockId

/**
 * Contains the logic and parameters necessary to write the block specified by
 * `serializedDataBlockId` (which should be cached in the MemoryStore) to the Hadoop Distributed
 * File System. Removes the block specified by `serializedDataBlockId` from the MemoryStore after
 * writing it to HDFS.
 *
 * Accepts a parameter `numRecords` that is the number of records that this HdfsWriteMonotask will
 * write to disk. Since this monotask only deals with serialized bytes, it has no way to determine
 * the number of records for itself. This is used to update the task metrics after the write
 * operation has completed.
 */
private[spark] class HdfsWriteMonotask(
    private val sparkTaskContext: TaskContextImpl,
    blockId: BlockId,
    private val serializedDataBlockId: BlockId,
    hadoopConf: Configuration,
    private val hadoopTaskContext: TaskAttemptContext,
    private val outputCommitter: FileOutputCommitter,
    private val codec: Option[CompressionCodec],
    private val numRecords: Long)
  extends HdfsDiskMonotask(sparkTaskContext, blockId, hadoopConf) with Logging {

  pathOpt = Some(constructPath(outputCommitter))

  /**
   * The buffer holding the data that this monotask will write to HDFS. Used to keep track of the
   * current position in the buffer from when it is first used in `initialize()`, which writes some
   * data to disk, to when the rest of the data is written in `execute()`.
   */
  private var bufferOpt: Option[ByteBuffer] = None

  /**
   * The stream used to write to an HDFS file. Used to pass the stream from `initialize()`, where it
   * is created, to `execute()`, where all but the first byte of data is written.
   */
  private var streamOpt: Option[FSDataOutputStream] = None

  /**
   * Used during a failure to determine whether the HDFS task corresponding to this monotask needs
   * to be aborted. This is set to `true` between when the HDFS task is setup and when it is
   * committed.
   */
  private var hadoopTaskInProgress = false

  /**
   * Initializes this HdfsWriteMonotask so that we can determine which disk its data will be
   * written to.  This requires writing a small amount (1 byte) of data to the file, so that the
   * file is created and we can query HDFS to see what disk the file is stored on. This must be
   * called before `execute()`.
   */
  def initialize(): Unit = {
    val buffer = SparkEnv.get.blockManager.getLocalBytes(serializedDataBlockId).getOrElse(
      throw new IllegalStateException(
        s"Writing block $blockId to HDFS failed because the block's serialized bytes " +
        s"(stored with blockId $serializedDataBlockId) could not be found.")).duplicate()
    buffer.rewind()
    bufferOpt = Some(buffer)

    outputCommitter.setupTask(hadoopTaskContext)
    hadoopTaskInProgress = true

    val path = getPath()
    val stream = path.getFileSystem(hadoopConf).create(path, false)
    streamOpt = Some(stream)

    if (buffer.hasRemaining()) {
      stream.writeByte(buffer.get())

      // We call stream.hflush() instead of stream.hsync() so that the one byte will (hopefully)
      // just be written to the OS buffer cache instead of the physical disk. The reason for this is
      // because we call this method from outside of a DiskMonotask and don't want to interfere with
      // any running DiskMonotasks.
      stream.hflush()
      logDebug(s"Created the HDFS file for block $blockId at location: ${path.toString()}")
    } else {
      // It is possible that this HdfsWriteMonotask is trying to write an empty file. This is
      // normal, but if this is the case, then we obviously cannot write the first byte of data to
      // force the file to be created. This is okay, since it does not matter which disk queue this
      // monotask ends up in, since the file is empty.
    }
  }

  override def execute(): Unit = {
    val buffer = bufferOpt.getOrElse(throw new IllegalStateException(
      "This HdfsWriteMonotask's's buffer has not been set. Make sure that the \"initialize()\" " +
      "method has been called."))
    val stream = streamOpt.getOrElse(throw new IllegalStateException(
      "This HdfsWriteMonotask's's stream has not been set. Make sure that the \"initialize()\" " +
        "method has been called."))

    if (buffer.hasRemaining()) {
      if (buffer.hasArray()) {
        // The ByteBuffer has a backing array that can be accessed without copying the bytes.
        // However, the backing array might be larger than the number of bytes that are stored in
        // the ByteBuffer, so we need to use the ByteBuffer's metadata to select only the relevant
        // portion of the array.
        //
        // The initialize() method already wrote the first byte of the file, so we skip it.
        stream.write(buffer.array(), 1, buffer.remaining())
      } else {
        val destByteArray = new Array[Byte](buffer.remaining())
        buffer.get(destByteArray)
        stream.write(destByteArray)
      }
    }

    stream.hsync()
    logInfo(s"Block $blockId (composed of $numRecords records) was successfully written to HDFS " +
      s"at location: ${getPath().toString()}")

    // The stream must be closed before the task is committed.
    stream.close()
    streamOpt = None
    SparkHadoopMapRedUtil.commitTask(outputCommitter, hadoopTaskContext, sparkTaskContext)
    hadoopTaskInProgress = false

    val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    sparkTaskContext.taskMetrics.outputMetrics = Some(outputMetrics)
    outputMetrics.setBytesWritten(buffer.limit())
    outputMetrics.setRecordsWritten(numRecords)
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
   * Removes the block specified by `serializedDataBlockId` from the MemoryStore, closes
   * `streamOpt` if necessary, and aborts the HDFS task if this HdfsWriteMonotask failed.
   */
  override protected def cleanupIntermediateData(): Unit = {
    super.cleanupIntermediateData()
    blockManager.removeBlockFromMemory(serializedDataBlockId, false)

    streamOpt.foreach { stream =>
      stream.close()
      streamOpt = None
    }
    if (hadoopTaskInProgress) {
      outputCommitter.abortTask(hadoopTaskContext)
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

package org.apache.spark.mapreduce

import java.io.{DataOutputStream, OutputStream}
import java.lang.{Boolean => JBoolean, Integer => JInteger}
import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem}
import org.apache.hadoop.fs.FileSystem.Statistics
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.mapreduce.{Job, JobContext, JobID, RecordWriter, TaskAttemptContext,
  TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat,
  SequenceFileOutputFormat, TextOutputFormat}
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.{InterruptibleIterator, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.monotasks.AddToMacrotask
import org.apache.spark.monotasks.disk.HdfsWriteMonotask
import org.apache.spark.storage.{BlockId, StorageLevel, TempLocalBlockId}
import org.apache.spark.util.{ByteArrayOutputStreamWithZeroCopyByteBuffer, LongArrayWritable}

private[spark]
trait SparkHadoopMapReduceUtil {
  def newJobContext(conf: Configuration, jobId: JobID): JobContext = {
    val klass = firstAvailableClass(
        "org.apache.hadoop.mapreduce.task.JobContextImpl",  // hadoop2, hadoop2-yarn
        "org.apache.hadoop.mapreduce.JobContext")           // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[JobID])
    ctor.newInstance(conf, jobId).asInstanceOf[JobContext]
  }

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = {
    val klass = firstAvailableClass(
        "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl",  // hadoop2, hadoop2-yarn
        "org.apache.hadoop.mapreduce.TaskAttemptContext")           // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[TaskAttemptID])
    ctor.newInstance(conf, attemptId).asInstanceOf[TaskAttemptContext]
  }

  def newTaskAttemptID(
      jtIdentifier: String,
      jobId: Int,
      isMap: Boolean,
      taskId: Int,
      attemptId: Int) = {
    val klass = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptID")
    try {
      // First, attempt to use the old-style constructor that takes a boolean isMap
      // (not available in YARN)
      val ctor = klass.getDeclaredConstructor(classOf[String], classOf[Int], classOf[Boolean],
        classOf[Int], classOf[Int])
      ctor.newInstance(jtIdentifier, new JInteger(jobId), new JBoolean(isMap), new JInteger(taskId),
        new JInteger(attemptId)).asInstanceOf[TaskAttemptID]
    } catch {
      case exc: NoSuchMethodException => {
        // If that failed, look for the new constructor that takes a TaskType (not available in 1.x)
        val taskTypeClass = Class.forName("org.apache.hadoop.mapreduce.TaskType")
          .asInstanceOf[Class[Enum[_]]]
        val taskType = taskTypeClass.getMethod("valueOf", classOf[String]).invoke(
          taskTypeClass, if(isMap) "MAP" else "REDUCE")
        val ctor = klass.getDeclaredConstructor(classOf[String], classOf[Int], taskTypeClass,
          classOf[Int], classOf[Int])
        ctor.newInstance(jtIdentifier, new JInteger(jobId), taskType, new JInteger(taskId),
          new JInteger(attemptId)).asInstanceOf[TaskAttemptID]
      }
    }
  }

  private def firstAvailableClass(first: String, second: String): Class[_] = {
    try {
      Class.forName(first)
    } catch {
      case e: ClassNotFoundException =>
        Class.forName(second)
    }
  }

  /**
   * Creates TaskAttemptContext, FileOutputFormat, FileOutputCommitter, and optional
   * CompressionCodec instances for the Hadoop task defined by the provided Configuration.
   * `sparkTaskContext` is optional so that this method can be used when the objects are not
   * associated with a particular task (e.g., in the driver code for a job that writes an RDD to
   * HDFS, see `PairRDDFunctions.saveAsNewAPIHadoopDataset()`).
   */
  def getHadoopTaskObjects(
      sparkTaskContext: Option[TaskContext],
      hadoopConf: Configuration,
      jobTrackerId: String,
      jobId: Int,
      isMap: Boolean): HadoopTaskObjects = {
    val (taskId, attemptId) = sparkTaskContext match {
      case Some(context) =>
        // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
        // around by taking a mod. We expect that no task will be attempted 2 billion times.
        val shortenedTaskId = (context.taskAttemptId() % Int.MaxValue).toInt
        (context.partitionId(), shortenedTaskId)

      case None =>
        (0, 0)
    }
    val hadoopTaskId = newTaskAttemptID(jobTrackerId, jobId, isMap, taskId, attemptId)
    val hadoopTaskContext = newTaskAttemptContext(hadoopConf, hadoopTaskId)

    val fileOutputFormat = Job.getInstance(hadoopConf).getOutputFormatClass().newInstance() match {
      case fileOutputFormat: FileOutputFormat[_, _] =>
        fileOutputFormat
      case otherFormat =>
        throw new UnsupportedOperationException("Unsupported OutputFormat: " +
          s"${otherFormat.getClass().getName()}. Only OutputFormats of type " +
          s"${classOf[FileOutputFormat[_, _]].getName()} are supported.")
    }
    fileOutputFormat match {
      case configurable: Configurable => configurable.setConf(hadoopConf)
      case _ =>
    }

    val fileOutputCommitter = fileOutputFormat.getOutputCommitter(hadoopTaskContext) match {
      case fileCommitter: FileOutputCommitter =>
        fileCommitter
      case otherCommitter =>
        throw new UnsupportedOperationException("Unsupported OutputCommitter: " +
          s"${otherCommitter.getClass().getName()}. Only OutputCommitters of type " +
          s"${classOf[FileOutputCommitter].getName()} are supported.")
    }

    val codec = if (FileOutputFormat.getCompressOutput(hadoopTaskContext)) {
      val codecClass =
        FileOutputFormat.getOutputCompressorClass(hadoopTaskContext, classOf[GzipCodec])
      Some(ReflectionUtils.newInstance(codecClass, hadoopConf).asInstanceOf[CompressionCodec])
    } else {
      None
    }

    new HadoopTaskObjects(hadoopTaskContext, fileOutputFormat, fileOutputCommitter, codec)
  }

  /**
   * Returns a RecordWriter corresponding to the provided FileOutputFormat that will output
   * serialized data to `byteStream`.
   */
  def getRecordWriter(
      byteStream: ByteArrayOutputStreamWithZeroCopyByteBuffer,
      hadoopConf: Configuration,
      hadoopTaskContext: TaskAttemptContext,
      hadoopOutputFormat: FileOutputFormat[_, _],
      codec: Option[CompressionCodec]): RecordWriter[Any, Any] = {
    hadoopOutputFormat match {
      case _: SequenceFileOutputFormat[_, _] =>
        getSequenceFileRecordWriter(byteStream, hadoopConf, hadoopTaskContext, codec)

      case teraOutputFormat: TeraOutputFormat =>
        getTeraRecordWriter(byteStream, hadoopConf, teraOutputFormat)

      case _: TextOutputFormat[_, _] =>
        getTextFileRecordWriter(byteStream, hadoopConf, codec)

      case _ =>
        throw new UnsupportedOperationException("Unsupported FileOutputFormat: " +
          s"${hadoopOutputFormat.getClass().getName()}. HdfsSerializationMonotask only supports " +
          s"${classOf[TextOutputFormat[_, _]].getName()}.")
    }
  }

  /**
   * This method was written to replicate the functionality in
   * SequenceFileOutputFormat.getRecordWriter(). We cannot directly use the Hadoop
   * SequenceFileOutputFormat methods because those methods tie together serialization and writing
   * the data to disk, which we need to separate.
   */
  private def getSequenceFileRecordWriter(
      byteStream: ByteArrayOutputStreamWithZeroCopyByteBuffer,
      hadoopConf: Configuration,
      hadoopTaskContext: TaskAttemptContext,
      codec: Option[CompressionCodec]): RecordWriter[Any, Any] = {
    val outputStream = createFsDataOutputStream(byteStream, hadoopConf)
    val keyClass = hadoopTaskContext.getOutputKeyClass()
    val valueClass = hadoopTaskContext.getOutputValueClass()
    val compressionType = codec.map { _ =>
      SequenceFileOutputFormat.getOutputCompressionType(hadoopTaskContext)
    }.getOrElse(CompressionType.NONE)

    val sequenceFileWriter = SequenceFile.createWriter(
      hadoopConf,
      Writer.stream(outputStream),
      Writer.keyClass(keyClass),
      Writer.valueClass(valueClass),
      Writer.compression(compressionType, codec.orNull))

    new RecordWriter[Any, Any]() {
      override def write(key: Any, value: Any): Unit =
        sequenceFileWriter.append(key, value)

      override def close(hadoopTaskContext: TaskAttemptContext): Unit =
        sequenceFileWriter.close()
    }
  }

  private def getTeraRecordWriter(
      byteStream: ByteArrayOutputStreamWithZeroCopyByteBuffer,
      hadoopConf: Configuration,
      teraOutputFormat: TeraOutputFormat): RecordWriter[Any, Any] = {
    new teraOutputFormat.TeraRecordWriter(
      createFsDataOutputStream(byteStream, hadoopConf),
      Job.getInstance(hadoopConf)).asInstanceOf[RecordWriter[Any, Any]]
  }

  private def getTextFileRecordWriter(
      byteStream: ByteArrayOutputStreamWithZeroCopyByteBuffer,
      hadoopConf: Configuration,
      codec: Option[CompressionCodec]): RecordWriter[Any, Any] = {
    val outputStream =
      new DataOutputStream(codec.map(_.createOutputStream(byteStream)).getOrElse(byteStream))

    // Default value chosen to be consistent with
    // org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.getRecordWriter()
    val separator = hadoopConf.get("mapreduce.output.textoutputformat.separator", "\t")
    new TextOutputFormat.LineRecordWriter[Any, Any](outputStream, separator)
  }

  /** Creates an FSDataOutputStream that forwards data to the provided OutputStream. */
  private def createFsDataOutputStream(
      outputStream: OutputStream,
      hadoopConf: Configuration): FSDataOutputStream =
    new FSDataOutputStream(outputStream, new Statistics(FileSystem.get(hadoopConf).getScheme()))

  /**
   * Schedules an HdfsWriteMonotask that will write `buffer` to HDFS. `buffer` is first cached in
   * the MemoryStore and then retrieved by the HdfsWriteMonotask.
   */
  def submitHdfsWriteMonotask(
      sparkTaskContext: TaskContext,
      blockId: BlockId,
      buffer: ByteBuffer,
      numRecords: Long,
      hadoopConf: Configuration,
      hadoopTaskContext: TaskAttemptContext,
      hadoopOutputCommitter: FileOutputCommitter,
      codec: Option[CompressionCodec]): Unit = {
    val serializedDataBlockId = new TempLocalBlockId(UUID.randomUUID())
    SparkEnv.get.blockManager.cacheBytes(
      serializedDataBlockId, buffer, StorageLevel.MEMORY_ONLY_SER, tellMaster = false)

    val sparkTaskContextImpl = sparkTaskContext match {
      case impl: TaskContextImpl =>
        impl
      case _ =>
        throw new UnsupportedOperationException(
          "Can only submit an HdfsWriteMonotask using a TaskContext of " +
          s"type ${classOf[TaskContextImpl].getName()}")
    }

    val hdfsWriteMonotask = new HdfsWriteMonotask(
      sparkTaskContextImpl,
      blockId,
      serializedDataBlockId,
      hadoopConf,
      hadoopTaskContext,
      hadoopOutputCommitter,
      codec,
      numRecords)
    SparkEnv.get.localDagScheduler.post(AddToMacrotask(hdfsWriteMonotask))
  }

  /** Unrolls the provided iterator, then returns a new iterator over the values. */
  def makeMaterializedWritableIterator[K, V](
      sparkTaskContext: TaskContext,
      it: Iterator[(K, V)]): InterruptibleIterator[(K, V)] = {
    val buffer = new ArrayBuffer[(K, V)]()
    while (it.hasNext) {
      val nextPair = it.next()
      buffer += ((
        copyWritable(nextPair._1.asInstanceOf[Writable]).asInstanceOf[K],
        copyWritable(nextPair._2.asInstanceOf[Writable]).asInstanceOf[V]))
    }
    new InterruptibleIterator[(K, V)](sparkTaskContext, buffer.iterator)
  }

  /** Returns a copy of the provided writable. */
  private def copyWritable(original: Writable): Writable = {
    original match {
      case longArrayWritable: LongArrayWritable =>
        new LongArrayWritable(longArrayWritable.get())
      case longWritable: LongWritable =>
        new LongWritable(longWritable.get())
      case nullWritable: NullWritable =>
        NullWritable.get()
      case text: Text =>
        new Text(text.copyBytes())
      case bytesWritable: BytesWritable =>
        new BytesWritable(bytesWritable.copyBytes())
      case other =>
        throw new Exception(s"Unknown Writable: $other (${other.getClass})")
    }
  }
}

/** Bundles together several commonly-used Hadoop objects. */
private[spark] class HadoopTaskObjects(
    val taskContext: TaskAttemptContext,
    val fileOutputFormat: FileOutputFormat[_, _],
    val fileOutputCommitter: FileOutputCommitter,
    val codec: Option[CompressionCodec])

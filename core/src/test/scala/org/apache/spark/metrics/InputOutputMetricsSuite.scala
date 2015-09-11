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

package org.apache.spark.metrics

import java.io.{File, FileWriter, PrintWriter}

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang.math.RandomUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.{CombineFileInputFormat => OldCombineFileInputFormat,
  CombineFileRecordReader => OldCombineFileRecordReader, CombineFileSplit => OldCombineFileSplit}
import org.apache.hadoop.mapred.{JobConf, Reporter, FileSplit => OldFileSplit,
  InputSplit => OldInputSplit, LineRecordReader => OldLineRecordReader,
  RecordReader => OldRecordReader, TextInputFormat => OldTextInputFormat}
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat => NewCombineFileInputFormat,
  CombineFileRecordReader => NewCombineFileRecordReader, CombineFileSplit => NewCombineFileSplit,
  FileSplit => NewFileSplit, TextInputFormat => NewTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}
import org.apache.hadoop.mapreduce.{TaskAttemptContext, InputSplit => NewInputSplit,
  RecordReader => NewRecordReader}
import org.scalatest.Assertions.assertResult
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SharedSparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

class InputOutputMetricsSuite extends FunSuite with SharedSparkContext
  with BeforeAndAfter {

  @transient var tmpDir: File = _
  @transient var tmpFile: File = _
  @transient var tmpFilePath: String = _
  @transient val numRecords: Int = 100000
  @transient val numBuckets: Int = 10

  before {
    tmpDir = Utils.createTempDir()
    val testTempDir = new File(tmpDir, "test")
    testTempDir.mkdir()

    tmpFile = new File(testTempDir, getClass.getSimpleName + ".txt")
    val pw = new PrintWriter(new FileWriter(tmpFile))
    for (x <- 1 to numRecords) {
      pw.println(RandomUtils.nextInt(numBuckets))
    }
    pw.close()

    // Path to tmpFile
    tmpFilePath = "file://" + tmpFile.getAbsolutePath
  }

  after {
    Utils.deleteRecursively(tmpDir)
  }

  test("input metrics for old hadoop with coalesce") {
    val bytesRead = runAndReturnBytesRead {
      sc.textFile(tmpFilePath, 4).count()
    }
    val bytesRead2 = runAndReturnBytesRead {
      sc.textFile(tmpFilePath, 4).coalesce(2).count()
    }
    assert(bytesRead != 0)
    assert(bytesRead == bytesRead2)
    assert(bytesRead2 >= tmpFile.length())
  }

  test("input metrics with cache and coalesce") {
    // prime the cache manager
    val rdd = sc.textFile(tmpFilePath, 4).cache()
    rdd.collect()

    val bytesRead = runAndReturnBytesRead {
      rdd.count()
    }
    val bytesRead2 = runAndReturnBytesRead {
      rdd.coalesce(4).count()
    }

    // for count and coelesce, the same bytes should be read.
    assert(bytesRead != 0)
    assert(bytesRead2 == bytesRead)
  }

  /**
   * This checks the situation where we have interleaved reads from
   * different sources. Currently, we only accumulate fron the first
   * read method we find in the task. This test uses cartesian to create
   * the interleaved reads.
   *
   * Once https://issues.apache.org/jira/browse/SPARK-5225 is fixed
   * this test should break.
   */
  test("input metrics with mixed read method") {
    // prime the cache manager
    val numPartitions = 2
    val rdd = sc.parallelize(1 to 100, numPartitions).cache()
    rdd.collect()

    val rdd2 = sc.textFile(tmpFilePath, numPartitions)

    val bytesRead = runAndReturnBytesRead {
      rdd.count()
    }
    val bytesRead2 = runAndReturnBytesRead {
      rdd2.count()
    }

    val cartRead = runAndReturnBytesRead {
      rdd.cartesian(rdd2).count()
    }

    assert(cartRead != 0)
    assert(bytesRead != 0)
    assert(bytesRead2 != 0)

    // The second RDD is read from first, so only its metrics are recorded.
    assertResult(bytesRead2 * numPartitions)(cartRead)
  }

  test("input metrics for new Hadoop API with coalesce") {
    val bytesRead = runAndReturnBytesRead {
      sc.newAPIHadoopFile(tmpFilePath, classOf[NewTextInputFormat], classOf[LongWritable],
        classOf[Text]).count()
    }
    val bytesRead2 = runAndReturnBytesRead {
      sc.newAPIHadoopFile(tmpFilePath, classOf[NewTextInputFormat], classOf[LongWritable],
        classOf[Text]).coalesce(5).count()
    }
    assert(bytesRead != 0)
    assert(bytesRead2 == bytesRead)
    assert(bytesRead >= tmpFile.length())
  }

  test("input metrics for a simple read") {
    var bytesRead = 0L
    var recordsRead = 0L
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = taskEnd.taskMetrics
        metrics.inputMetrics.foreach(bytesRead += _.bytesRead)
        metrics.inputMetrics.foreach(recordsRead += _.recordsRead)
      }
    })

    sc.textFile(tmpFilePath, 4).count()
    sc.listenerBus.waitUntilEmpty(500)

    assertResult(tmpFile.length())(bytesRead)
    assertResult(numRecords)(recordsRead)
  }

  test("input metrics for a read with more stages") {
    var bytesRead = 0L
    var recordsRead = 0L
    var shuffleBytesRead = 0L
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = taskEnd.taskMetrics
        metrics.inputMetrics.foreach(bytesRead += _.bytesRead)
        metrics.inputMetrics.foreach(recordsRead += _.recordsRead)
        metrics.shuffleReadMetrics.foreach(shuffleBytesRead += _.localBytesRead)
      }
    })

    sc.textFile(tmpFilePath, 4)
      .map(key => (key.length, 1))
      .reduceByKey(_ + _)
      .count()
    sc.listenerBus.waitUntilEmpty(500)

    // The total number of bytes that are read is equal to the size of the input file (which is read
    // by HdfsWriteMonotasks) plus the number of shuffle bytes read by DiskReadMonotasks.
    assertResult(tmpFile.length() + shuffleBytesRead)(bytesRead)
    assertResult(numRecords)(recordsRead)
  }

  test("input metrics on records read with cache") {
    // prime the cache manager
    val rdd = sc.textFile(tmpFilePath, 4).cache()
    rdd.collect()

    val records = runAndReturnRecordsRead {
      rdd.count()
    }

    assert(records == numRecords)
  }

  test("shuffle records read metrics") {
    val recordsRead = runAndReturnShuffleRecordsRead {
      sc.textFile(tmpFilePath, 1)
        .map(key => (key, 1))
        .groupByKey()
        .collect()
    }
    assert(recordsRead == numRecords)
  }

  test("shuffle records written metrics") {
    val recordsWritten = runAndReturnShuffleRecordsWritten {
      sc.textFile(tmpFilePath, 4)
        .map(key => (key, 1))
        .groupByKey()
        .collect()
    }
    assert(recordsWritten == numRecords)
  }

  /**
   * Tests the metrics from end to end.
   * 1) reading from a hadoop file
   * 2) shuffle
   * 3) writing to a hadoop file.
   */
  test("input read/write and shuffle read/write metrics all line up") {
    var recordsRead = 0L
    var bytesRead = 0L
    var outputRecordsWritten = 0L
    var shuffleBytesRead = 0L
    var shuffleRecordsRead = 0L
    var shuffleRecordsWritten = 0L
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = taskEnd.taskMetrics
        metrics.inputMetrics.foreach(recordsRead += _.recordsRead)
        metrics.inputMetrics.foreach(bytesRead += _.bytesRead)
        metrics.outputMetrics.foreach(outputRecordsWritten += _.recordsWritten)
        metrics.shuffleReadMetrics.foreach(shuffleBytesRead += _.localBytesRead)
        metrics.shuffleReadMetrics.foreach(shuffleRecordsRead += _.recordsRead)
        metrics.shuffleWriteMetrics.foreach(shuffleRecordsWritten += _.shuffleRecordsWritten)
      }
    })

    val tmpFile2 = new File(tmpDir, s"${getClass().getSimpleName()}_2.txt")
    sc.textFile(tmpFilePath, 4)
      .map(key => (key, 1))
      .reduceByKey(_+_)
      .saveAsNewApiTextFile("file://" + tmpFile2.getAbsolutePath())

    sc.listenerBus.waitUntilEmpty(500)

    // The total number of bytes that are read is equal to the size of the input file (which is read
    // by HdfsWriteMonotasks) plus the number of shuffle bytes read by DiskReadMonotasks.
    assertResult(tmpFile.length() + shuffleBytesRead)(bytesRead)
    assertResult(numRecords)(recordsRead)

    assertResult(numBuckets)(outputRecordsWritten)
    assert(shuffleRecordsRead === shuffleRecordsWritten)
  }

  test("input metrics with interleaved reads") {
    val numWritePartitions1 = 2
    val numWritePartitions2 = 5

    val prefix = s"${getClass().getSimpleName()}_cart_"
    val cartFilePath1 = s"file://${new File(tmpDir, s"${prefix}_1.txt").getAbsolutePath()}"
    val cartFilePath2 = s"file://${new File(tmpDir, s"${prefix}_2.txt").getAbsolutePath()}"

    // Write files to disk so we can read them later.
    sc.parallelize(1 to 10, numWritePartitions1).saveAsNewApiTextFile(cartFilePath1)
    sc.parallelize(1 to 1000, numWritePartitions2).saveAsNewApiTextFile(cartFilePath2)

    val rdd1 = sc.textFile(cartFilePath1)
    val rdd2 = sc.textFile(cartFilePath2)

    val size1Bytes = runAndReturnBytesRead {
      rdd1.count()
    }
    val size2Bytes = runAndReturnBytesRead {
      rdd2.count()
    }
    val cartesianBytes = runAndReturnBytesRead {
      rdd1.cartesian(rdd2).count()
    }

    // The first file is read once for every partition of the second file, and the second file is
    // read once for every partition of the first file.
    //
    // TODO: When functionality that prevents an Executor from loading the same block multiple times
    //       simultaneously is implemented, the formula will be: size1Bytes + size2Bytes
    assertResult(size1Bytes * numWritePartitions2 + size2Bytes * numWritePartitions1)(
      cartesianBytes)
  }

  private def runAndReturnBytesRead(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.inputMetrics.map(_.bytesRead))
  }

  private def runAndReturnRecordsRead(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.inputMetrics.map(_.recordsRead))
  }

  private def runAndReturnRecordsWritten(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.outputMetrics.map(_.recordsWritten))
  }

  private def runAndReturnShuffleRecordsRead(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.shuffleReadMetrics.map(_.recordsRead))
  }

  private def runAndReturnShuffleRecordsWritten(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.shuffleWriteMetrics.map(_.shuffleRecordsWritten))
  }

  private def runAndReturnNumUpdatedBlocks(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.updatedBlocks.map(_.size.toLong))
  }

  private def runAndReturnMetrics(job: => Unit,
      collector: (SparkListenerTaskEnd) => Option[Long]): Long = {
    val taskMetrics = new ArrayBuffer[Long]()
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        collector(taskEnd).foreach(taskMetrics += _)
      }
    })

    job

    sc.listenerBus.waitUntilEmpty(500)
    taskMetrics.sum
  }

  /**
   * TODO: This test case is ignored because the org.apache.hadoop.mapred library is no longer
   *       supported. This test should be re-enabled once support for the org.apache.hadoop.mapred
   *       library has been refactored to use monotasks.
   */
  ignore("output metrics on records written") {
    // Only supported on newer Hadoop
    if (SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback().isDefined) {
      val file = new File(tmpDir, getClass.getSimpleName)
      val filePath = "file://" + file.getAbsolutePath

      val records = runAndReturnRecordsWritten {
        sc.parallelize(1 to numRecords).saveAsTextFile(filePath)
      }
      assert(records == numRecords)
    }
  }

  test("output metrics on records written - new Hadoop API") {
    val file = new File(tmpDir, getClass.getSimpleName)
    val filePath = "file://" + file.getAbsolutePath

    val records = runAndReturnRecordsWritten {
      sc.parallelize(1 to numRecords).map(key => (key.toString, key.toString))
        .saveAsNewAPIHadoopFile[NewTextOutputFormat[String, String]](filePath)
    }
    assert(records == numRecords)
  }

  test("output metrics when writing text file") {
    val fs = FileSystem.getLocal(new Configuration())
    val outPath = new Path(fs.getWorkingDirectory, "outdir")

    val taskBytesWritten = new ArrayBuffer[Long]()
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        taskBytesWritten += taskEnd.taskMetrics.outputMetrics.get.bytesWritten
      }
    })

    val rdd = sc.parallelize(Array("a", "b", "c", "d"), 2)

    try {
      rdd.saveAsNewApiTextFile(outPath.toString)
      sc.listenerBus.waitUntilEmpty(500)
      assert(taskBytesWritten.length == 2)
      val outFiles = fs.listStatus(outPath).filter(_.getPath.getName != "_SUCCESS")
      taskBytesWritten.zip(outFiles).foreach { case (bytes, fileStatus) =>
        assert(bytes >= fileStatus.getLen)
      }
    } finally {
      fs.delete(outPath, true)
    }
  }

  /**
   * TODO: This test case is ignored because the org.apache.hadoop.mapred library is no longer
   *       supported. This test should be re-enabled once support for the org.apache.hadoop.mapred
   *       library has been refactored to use monotasks.
   */
  ignore("input metrics with old CombineFileInputFormat") {
    val bytesRead = runAndReturnBytesRead {
      sc.hadoopFile(tmpFilePath, classOf[OldCombineTextInputFormat], classOf[LongWritable],
        classOf[Text], 2).count()
    }
    assert(bytesRead >= tmpFile.length())
  }

  /**
   * TODO: This test case is ignored because the new monotasks-based interface with HDFS does not
   *       support CombineFileInputFormat.
   */
  ignore("input metrics with new CombineFileInputFormat") {
    val bytesRead = runAndReturnBytesRead {
      sc.newAPIHadoopFile(tmpFilePath, classOf[NewCombineTextInputFormat], classOf[LongWritable],
        classOf[Text], new Configuration()).count()
    }
    assert(bytesRead >= tmpFile.length())
  }

  test("TaskMetrics.updatedBlocks is updated correctly when persisting RDDs to disk") {
    val numPartitions = 16
    val numUpdatedBlocks = runAndReturnNumUpdatedBlocks {
      val rddA = sc.parallelize(1 to numPartitions, numPartitions)
      val rddB = rddA.map(i => i * i).persist(StorageLevel.DISK_ONLY)
      val rddC = rddB.map(i => i * i).persist(StorageLevel.DISK_ONLY)
      rddC.count()
    }
    assert(numUpdatedBlocks === numPartitions * 2)
  }
}

/**
 * Hadoop 2 has a version of this, but we can't use it for backwards compatibility
 */
class OldCombineTextInputFormat extends OldCombineFileInputFormat[LongWritable, Text] {
  override def getRecordReader(split: OldInputSplit, conf: JobConf, reporter: Reporter)
  : OldRecordReader[LongWritable, Text] = {
    new OldCombineFileRecordReader[LongWritable, Text](conf,
      split.asInstanceOf[OldCombineFileSplit], reporter, classOf[OldCombineTextRecordReaderWrapper]
        .asInstanceOf[Class[OldRecordReader[LongWritable, Text]]])
  }
}

class OldCombineTextRecordReaderWrapper(
    split: OldCombineFileSplit,
    conf: Configuration,
    reporter: Reporter,
    idx: Integer) extends OldRecordReader[LongWritable, Text] {

  val fileSplit = new OldFileSplit(split.getPath(idx),
    split.getOffset(idx),
    split.getLength(idx),
    split.getLocations())

  val delegate: OldLineRecordReader = new OldTextInputFormat().getRecordReader(fileSplit,
    conf.asInstanceOf[JobConf], reporter).asInstanceOf[OldLineRecordReader]

  override def next(key: LongWritable, value: Text): Boolean = delegate.next(key, value)
  override def createKey(): LongWritable = delegate.createKey()
  override def createValue(): Text = delegate.createValue()
  override def getPos(): Long = delegate.getPos
  override def close(): Unit = delegate.close()
  override def getProgress(): Float = delegate.getProgress
}

/**
 * Hadoop 2 has a version of this, but we can't use it for backwards compatibility
 */
class NewCombineTextInputFormat extends NewCombineFileInputFormat[LongWritable,Text] {
  def createRecordReader(split: NewInputSplit, context: TaskAttemptContext)
  : NewRecordReader[LongWritable, Text] = {
    new NewCombineFileRecordReader[LongWritable,Text](split.asInstanceOf[NewCombineFileSplit],
      context, classOf[NewCombineTextRecordReaderWrapper])
  }
}

class NewCombineTextRecordReaderWrapper(
    split: NewCombineFileSplit,
    context: TaskAttemptContext,
    idx: Integer) extends NewRecordReader[LongWritable, Text] {

  val fileSplit = new NewFileSplit(split.getPath(idx),
    split.getOffset(idx),
    split.getLength(idx),
    split.getLocations())

  val delegate = new NewTextInputFormat().createRecordReader(fileSplit, context)

  override def initialize(split: NewInputSplit, context: TaskAttemptContext): Unit = {
    delegate.initialize(fileSplit, context)
  }

  override def nextKeyValue(): Boolean = delegate.nextKeyValue()
  override def getCurrentKey(): LongWritable = delegate.getCurrentKey
  override def getCurrentValue(): Text = delegate.getCurrentValue
  override def getProgress(): Float = delegate.getProgress
  override def close(): Unit = delegate.close()
}

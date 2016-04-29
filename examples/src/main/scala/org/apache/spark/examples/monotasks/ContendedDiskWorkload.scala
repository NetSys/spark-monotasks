/*
 * Copyright 2016 The Regents of The University California
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

package org.apache.spark.examples.monotasks

import java.net.URI

import scala.language.postfixOps
import scala.sys.process._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.Random

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.util.{LongArrayWritable, Utils}

/**
 * Runs two jobs concurrently. The first job uses only the disk while the second job, which starts a
 * few seconds later, uses the disk and then the CPU. The purpose of this experiment is to evaluate
 * how effectively the task scheduling facilities of Spark and Monotasks are able to keep both the
 * disk and the CPU highly utilized. Since the first job's tasks arrive at the disk before the
 * second job's tasks, it is possible for the CPU to sit idle as the second job's tasks wait for
 * their turn to use the CPU. We want to avoid this scenario. Instead, both jobs should share the
 * disk in a way that allows the CPU to be utilized as well.
 *
 * In order to demonstrate the above problem in the most egregious way, the CPU should be the second
 * job's bottleneck. In order to accomplish this, the second job performs random computations during
 * the CPU phase for a user-configurable amount of time. Adjusting the amount of time spent
 * computing during each task changes whether the bottleneck of the second job is the CPU or the
 * disk.
 */
object ContendedDiskWorkload extends Logging {

  val usage = "<num workers> <num partitions> <items per partition> " +
    "<items per value> <delay between jobs (ms)> <CPU time per task (ms)> <num trials>"

  def main(args: Array[String]) {
    if (args.length != 7) {
      throw new Exception(s"Incorrect number of arguments! Usage: $usage")
    }
    val numWorkers = args(0).toInt
    val numPartitions = args(1).toInt
    val itemsPerPartition = args(2).toInt
    val itemsPerValue = args(3).toInt
    val jobDelayMillis = args(4).toInt
    val perTaskCpuMillis = args(5).toFloat
    val numTrials = args(6).toInt

    val perItemCpuNanos = ((perTaskCpuMillis / itemsPerPartition) * 1000000L).toLong
    logInfo(s"The second job will compute for $perItemCpuNanos nanoseconds per item.")

    val spark = new SparkContext(new SparkConf().setAppName("Contended Disk Workload"))
    val hdfs = FileSystem.get(spark.hadoopConfiguration)

    var dataUris: Seq[String] = Seq.empty
    try {
      // The jobs must use separate files so that when the jobs are both reading, one does not
      // just read buffer cache data retrieved by the other job.
      val dataUris = createDataFiles(
        spark, hdfs, numPartitions, itemsPerPartition, itemsPerValue)

      (1 to numTrials).foreach { _ =>
        spark.parallelize(1 to numWorkers, numWorkers).foreach { _ =>
          // Force a garbage collection to happen, in order to try to avoid long garbage
          // collections in the middle of the jobs.
          System.gc()
          // Flush the buffer cache to prevent this trial from reading data retrieved by the
          // previous trial.
          "/root/spark-ec2/clear-cache.sh" !
        }

        runJobs(spark, dataUris, jobDelayMillis, perItemCpuNanos)
      }
    } finally {
      // Be sure to always stop the SparkContext, even when an exception is thrown; otherwise, the
      // event logs are more difficult to access.
      spark.stop()

      // Delete all of the test data.
      dataUris.foreach(dataUri => hdfs.delete(new Path(dataUri), true))
    }
  }

  /**
   * Generates two RDDs using the provided parameters and saves them to HDFS. Returns the paths to
   * the two files.
   */
  private def createDataFiles(
      spark: SparkContext,
      hdfs: FileSystem,
      numPartitions: Int,
      itemsPerPartition: Int,
      itemsPerValue: Int): Seq[String] = {
    (1 to 2).map { suffix =>
      val dataUri = (s"hdfs:///" +
        s"ContendedDiskWorkload_${numPartitions}_${itemsPerPartition}_${itemsPerValue}_$suffix")

      spark.parallelize(1 to numPartitions, numPartitions).flatMap { i =>
        val random = new Random(i)
        Array.fill(itemsPerPartition)((
          new LongWritable(random.nextLong()),
          new LongArrayWritable(Array.fill(itemsPerValue)(random.nextLong()))))
      }.saveAsNewAPIHadoopFile[SequenceFileOutputFormat[LongWritable, LongArrayWritable]](dataUri)

      logInfo(s"Created new dataset at location $dataUri with " +
        s"size: ${Utils.bytesToString(hdfs.getFileStatus(new Path(dataUri)).getLen())}")
      dataUri
    }.toSeq
  }

  /**
   * Runs two jobs concurrently, one that reads data from disk and one that reads data from disk and
   * then computes on it. The second job starts jobDelayMillis after the first job. dataUris should
   * be a two-item sequence containing the paths to the files that the two jobs will read.
   * perItemCpuNanos controls how much time the second job will spend computing per item of the
   * RDD.
   */
  private def runJobs(
      spark: SparkContext,
      dataUris: Seq[String],
      jobDelayMillis: Int,
      perItemCpuNanos: Long): Unit = {
    import ExecutionContext.Implicits.global
    val firstJobFuture = Future(runJob(spark, dataUris(0)))
    val secondJobFuture = Future {
      // Sleep to allow the first job to start and fill the CPU queue with its tasks.
      Thread.sleep(jobDelayMillis)
      runJob(spark, dataUris(1), perItemCpuNanos)
    }

    val waitTime = Duration(10, MINUTES)
    logInfo(s"Waiting $waitTime for both jobs to finish")
    Await.result(firstJobFuture, waitTime)
    Await.result(secondJobFuture, waitTime)
  }

  /**
   * Runs a job that reads an RDD from disk and counts it. If perItemCpuNanos is positive, then
   * the job also computes on the RDD before counting it.
   */
  private def runJob(spark: SparkContext, dataUri: String, perItemCpuNanos: Long = 0): Unit = {
    val useCpu = perItemCpuNanos > 0
    val description = s"disk${if (useCpu) " -> CPU" else ""}"
    logInfo(s"Running $description job in thread ${Thread.currentThread().getName}")

    val rddToRead = spark.sequenceFile(dataUri, classOf[LongWritable], classOf[LongArrayWritable])
    val rddToCount = if (useCpu) {
      rddToRead.map {
        case (key: LongWritable, value: LongArrayWritable) =>
          val endTimeNanos = System.nanoTime() + perItemCpuNanos
          // Perform random computations to achieve 100% CPU utilization for a user-defined amount
          // of time.
          var result = 0
          while (System.nanoTime() < endTimeNanos) {
            result = Math.tan(key.get()).toInt
          }
          result
      }
    } else {
      rddToRead
    }
    rddToCount.count()
  }
}

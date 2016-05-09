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

import scala.language.postfixOps
import scala.sys.process._

import scala.util.Random

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.util.{LongArrayWritable, Utils}

/**
 * Runs a simple job that reads data from HDFS and then does a user-defined amount of random
 * computation.
 */
object DiskCpuJob extends Logging {

  val usage = "<num workers> <num partitions> <items per partition> <items per value> " +
    "<CPU iterations per item> <num trials>"

  def main(args: Array[String]) {
    if (args.length != 6) {
      throw new Exception(s"Incorrect number of arguments! Usage: $usage")
    }
    val numWorkers = args(0).toInt
    val numPartitions = args(1).toInt
    val itemsPerPartition = args(2).toInt
    val itemsPerValue = args(3).toInt
    val cpuIterationsPerItem = args(4).toInt
    val numTrials = args(5).toInt

    val spark = new SparkContext(new SparkConf().setAppName("Disk -> CPU Job"))
    // Sleep to let all of the executors register.
    Thread.sleep(10000L)

    var dataUriOpt: Option[String] = None
    try {
      val dataUriOpt = Some(createDataFile(spark, numPartitions, itemsPerPartition, itemsPerValue))

      (1 to numTrials).foreach { _ =>
        spark.parallelize(1 to numWorkers, numWorkers).foreach { _ =>
          // Force a garbage collection to happen, in order to try to avoid long garbage
          // collections in the middle of the jobs.
          System.gc()
          // Flush the buffer cache to prevent this trial from reading data retrieved by the
          // previous trial.
          "/root/spark-ec2/clear-cache.sh" !
        }

        runJob(spark, dataUriOpt.get, cpuIterationsPerItem)
      }
    } finally {
      // Be sure to always stop the SparkContext, even when an exception is thrown; otherwise, the
      // event logs are more difficult to access.
      spark.stop()

      // Delete all of the test data.
      dataUriOpt.foreach(dataUri => s"/root/ephemeral-hdfs/bin/hdfs dfs -rm -r -f $dataUri" !)
    }
  }

  /**
   * Generates an RDD using the provided parameters and saves it to HDFS. Returns the path to the
   * HDFS file.
   */
  private def createDataFile(
      spark: SparkContext,
      numPartitions: Int,
      itemsPerPartition: Int,
      itemsPerValue: Int): String = {
    val dataUri = (s"hdfs:///" +
      s"ContendedDiskWorkload_${numPartitions}_${itemsPerPartition}_${itemsPerValue}")

    spark.parallelize(1 to numPartitions, numPartitions).flatMap { i =>
      val random = new Random(i)
      Array.fill(itemsPerPartition)((
        new LongWritable(random.nextLong()),
        new LongArrayWritable(Array.fill(itemsPerValue)(random.nextLong()))))
    }.saveAsNewAPIHadoopFile[SequenceFileOutputFormat[LongWritable, LongArrayWritable]](dataUri)

    val hdfs = FileSystem.get(spark.hadoopConfiguration)
    logInfo(s"Created new dataset at location $dataUri with " +
      s"size: ${Utils.bytesToString(hdfs.getFileStatus(new Path(dataUri)).getLen())}")
    dataUri
  }

  /**
   * Runs a job that reads an RDD from disk and then performs a user-defined amount of computation
   * for each item.
   */
  private def runJob(spark: SparkContext, dataUri: String, cpuIterationsPerItem: Int = 0): Unit = {
    spark.sequenceFile(dataUri, classOf[LongWritable], classOf[LongArrayWritable]).map { _ =>
      // Perform random computations to achieve 100% CPU utilization.
      var result = 0L
      (1 to cpuIterationsPerItem).foreach { i =>
        result += Math.tan(i).toLong
      }
      result
    }.count()
  }
}

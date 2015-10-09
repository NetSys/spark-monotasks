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

package org.apache.spark.examples.monotasks

import scala.concurrent.{Await, ExecutionContext, future}
import scala.concurrent.duration.{Duration, MINUTES}

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.util.LongArrayWritable

/**
 * Runs two jobs concurrently that each sort data from HDFS and store the resulting sorted data
 * back in HDFS.  The number of reduce tasks to use and filename of each file to sort are accepted
 * as command line parameters.
 */
object ConcurrentSortJobs extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Concurrent Sort Jobs")
    val spark = new SparkContext(conf)

    val numReduceTasks = args(0).toInt
    val firstFilename = args(1)
    val secondFilename = args(2)

    try {
      import ExecutionContext.Implicits.global
      val firstJobFuture = future {
        doSortJob(spark, numReduceTasks, s"First sort job $firstFilename", "pool1", firstFilename)
      }

      val secondJobFuture = future {
        doSortJob(
          spark, numReduceTasks, s"Second sort job $secondFilename", "pool2", secondFilename)
      }

      val waitTime = Duration(100, MINUTES)
      logInfo(s"Waiting $waitTime for both sort jobs to finish")
      Await.result(firstJobFuture, waitTime)
      Await.result(secondJobFuture, waitTime)

    } finally {
      // Be sure to always stop the SparkContext, even when an exception is thrown; otherwise, the
      // event logs are more difficult to access.
      spark.stop()
    }
  }

  def doSortJob(
      spark: SparkContext,
      numReduceTasks: Int,
      description: String,
      pool: String,
      filename: String): Unit = {
    logInfo(s"Running job $description in thread ${Thread.currentThread().getName}")
    val unsortedRddDisk = spark.sequenceFile(
      filename, classOf[LongWritable], classOf[LongArrayWritable])
    // Convert the RDD back to Longs, because LongWritables aren't serializable, so Spark
    // can't serialize them for the shuffle.
    val unsortedRddLongs = unsortedRddDisk.map { pair =>
      (pair._1.get(), pair._2.get())
    }
    val partitioner = new LongPartitioner(numReduceTasks)
    val sortedRdd = new ShuffledRDD[Long, Array[Long], Array[Long]](
      unsortedRddLongs, partitioner)
      .setKeyOrdering(Ordering[Long])
      .map(pair => (new LongWritable(pair._1), new LongArrayWritable(pair._2)))
    sortedRdd.saveAsNewAPIHadoopFile[SequenceFileOutputFormat[LongWritable, LongArrayWritable]](
      s"${filename}_sorted")
  }
}

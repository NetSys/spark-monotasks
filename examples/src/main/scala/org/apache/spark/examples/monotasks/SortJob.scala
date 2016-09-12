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

import scala.language.postfixOps
import scala.sys.process._
import scala.util.Random

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.util.LongArrayWritable

/**
 * Job that generates random data and stores the data in HDFS, and then reads the data back and
 * repeatedly shuffles it.
 */
object SortJob extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Sort Job")
    // Manually set Spark to write to disk, to overwrite any existing configuration that specifies
    // that shuffle data should be stored in-memory.
    conf.set("spark.shuffle.writeToDisk", "true")
    val spark = new SparkContext(conf)

    val numMapTasks = if (args.length > 0) args(0).toInt else 16
    val numReduceTasks = if (args.length > 1) args(1).toInt else 128
    val itemsPerPartition = if (args.length > 2) args(2).toInt else 6400000
    val itemsPerValue = if (args.length > 3) args(3).toInt else 6
    val numShuffles = if (args.length > 4) args(4).toInt else 10
    val maybeInputFilename = if (args.length > 5) Some(args(5)) else None
    val readExistingData = if (args.length > 6) args(6).toBoolean else false
    // Caching the HDFS input data and the resulting sorted RDD allows us to easily compare runtimes
    // for cached and uncached data using exactly the same input data.
    val cacheInputOutputData = if (args.length > 7) args(7).toBoolean else false

    // Sleep for a few seconds to give all of the executors a chance to register. Without this
    // sleep, the first stage can get scheduled before all of the executors have registered,
    // leading to load imbalance.
    Thread.sleep(5000)

    try {
      val filename = maybeInputFilename match {
        case Some(inputFilename) =>
          inputFilename

        case None =>
          s"${System.currentTimeMillis()}_randomData"
      }

      if (readExistingData) {
        logInfo(s"Skipping data generation and instead reading data from $filename")
      } else {
        // Create random data and write it to disk.
        logInfo(s"Generating random data and storing it in $filename")
        val unsortedRdd = spark.parallelize(1 to numMapTasks, numMapTasks).flatMap { i =>
          val random = new Random(i)
          Array.fill(itemsPerPartition)((
            new LongWritable(random.nextLong),
            new LongArrayWritable(Array.fill(itemsPerValue)(random.nextLong))))
        }
        unsortedRdd.saveAsNewAPIHadoopFile[
          SequenceFileOutputFormat[LongWritable, LongArrayWritable]](filename)
      }

      val unsortedRdd = spark.sequenceFile(
        filename, classOf[LongWritable], classOf[LongArrayWritable])
      if (cacheInputOutputData) {
        // Cache the input data in memory before running any shuffles.
        unsortedRdd.cache()
        unsortedRdd.count()
      }

      (0 until numShuffles).foreach { i =>
        // Remove the sorted data, so that the disk doesn't fill up, and just in case there's
        // lingering sorted data from an earlier experiment.
        "/root/ephemeral-hdfs/bin/hdfs dfs -rm -r ./*sorted*" !

        // Convert the RDD back to Longs, because LongWritables aren't serializable, so Spark
        // can't serialize them for the shuffle.
        val unsortedRddLongs = unsortedRdd.map { pair =>
          (pair._1.get(), pair._2.get())
        }
        val partitioner = new LongPartitioner(numReduceTasks)
        val sortedRdd = new ShuffledRDD[Long, Array[Long], Array[Long]](
          unsortedRddLongs, partitioner)
          .setKeyOrdering(Ordering[Long])
          .map(pair => (new LongWritable(pair._1), new LongArrayWritable(pair._2)))

        if (cacheInputOutputData) {
          // Cache the sorted RDD in memory. This data is then removed so that we do not fill up the
          // cluster's memory when running multiple shuffles.
          sortedRdd.cache()
          sortedRdd.count()
          sortedRdd.unpersist(blocking=true)
        } else {
          sortedRdd
            .saveAsNewAPIHadoopFile[SequenceFileOutputFormat[LongWritable, LongArrayWritable]](
              s"${filename}_sorted_$i")
        }

        // Force a garbage collection to happen, in order to try to avoid long garbage
        // collections in the middle of jobs.
        val numExecutors = spark.getExecutorStorageStatus.size - 1
        logInfo(s"Running GC job with $numExecutors tasks")
        spark.parallelize(1 to numExecutors, numExecutors).foreach { i =>
          System.gc()
        }
      }
    } finally {
      // Be sure to always stop the SparkContext, even when an exception is thrown; otherwise, the
      // event logs are more difficult to access.
      spark.stop()
    }
  }
}

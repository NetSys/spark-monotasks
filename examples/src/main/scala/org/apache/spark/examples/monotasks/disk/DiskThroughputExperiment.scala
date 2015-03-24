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

package org.apache.spark.examples.monotasks.disk

import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

/**
 * Runs simple jobs that write and read randomly generated data.
 *
 * Generates RDDs filled with randomly generated data, writes them to disk a number of times, then
 * reads them back. All test RDDs are written to disk before any are read back in order to decrease
 * the likelihood that some are read from the OS buffer cache, instead of the physical disk.
 *
 * Parameters:
 *   number of partitions in a test RDD
 *   items per partition
 *   Longs per item
 *     Each item is an array of Longs. This specifies the length of each array.
 *   number of RDDs
 *     The number of test RDDs to write and read. Each test RDD contains the same data, which is
 *     generated one time, before any writes begin. This number should be large enough so that the
 *     total amount of data that is written is larger than the machine's memory. This guarantees
 *     that the tests overwhelm the OS buffer cache (most likely causing cascading cache misses),
 *     which is a requirement in order to get accurate disk throughput results.
 */
object DiskThroughputExperiment {

  def main(args: Array[String]) {
    val numPartitions = if (args.length > 0) args(0).toInt else 16
    val itemsPerPartition = if (args.length > 1) args(1).toInt else 5000000
    val longsPerItem = if (args.length > 2) args(2).toInt else 6
    val numRdds = if (args.length > 3) args(3).toInt else 18

    println("Running disk throughput experiment...")
    println()
    println("Test parameters:")
    println(s"\tnumber of partitions: $numPartitions")
    println(s"\titems per partition: $itemsPerPartition")
    println(s"\tLongs per item: $longsPerItem")
    println(s"\tnumber of RDDs: $numRdds")
    println()

    val spark = new SparkContext(new SparkConf().setAppName(s"Disk Throughput Experiment"))
    val rddA = spark.parallelize(1 to numPartitions, numPartitions).flatMap { i =>
      val rand = new Random(i)
      Array.fill(itemsPerPartition)(Array.fill(longsPerItem)(rand.nextLong()))
    }.persist(StorageLevel.MEMORY_ONLY)
    println(s"Done generating RDD contents: ${rddA.count()}")

    (1 to numRdds).map { i =>
      val rddB = rddA.map(a => a).persist(StorageLevel.DISK_ONLY)
      println(s"Done writing RDD $i of $numRdds: ${rddB.count()}")
      (i, rddB)
    }.foreach { case(i, rddB) =>
      println(s"Done reading RDD $i of $numRdds: ${rddB.count()}")
    }

    spark.stop()
    println("Done with disk throughput experiment.")
  }
}

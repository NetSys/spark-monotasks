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

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Tests the Monotasks HDFS API by verifying the end-to-end correctness of writing and reading
 * sequence files and text files.
 */
object HdfsWriteReadTests {

  private var spark: SparkContext = _

  /**
   * A list of test functions to exercise. Each test function should return the RDD created by
   * writing the input RDD to disk and reading it back.
   */
  private val tests: Seq[(RDD[Int], String) => RDD[Int]] = Seq(testSequenceFiles, testTextFiles)

  /**
   * Executes the test functions in `tests`.
   *
   * @param numInts The number of elements (integers) in the test RDD. Be careful of setting this
   *                too high, because the entire RDD needs to be able to fit in the driver's memory
   *                (during the correctness check).
   * @param numPartitions The number of partitions in the test RDD.
   * @param testDir The HDFS directory in which to save the test files.
   */
  def main(args: Array[String]): Unit = {
    if (args.size != 3) {
      throw new IllegalArgumentException(
        "Arguments: <number of Ints in test RDD> <number of partitions> <test output directory>")
    }
    val numInts = args(0).toInt
    val numPartitions = args(1).toInt
    val testDir = args(2)

    println(s"Writing test data to HDFS directory: $testDir")
    spark = new SparkContext(new SparkConf().setAppName(getClass().getName()))

    // Create a directory to write the test files too, which we will delete later.
    val tempTestDir = s"$testDir/HdfsWriteReadTest"
    val tempPath = new Path(tempTestDir)
    val fileSystem = tempPath.getFileSystem(spark.hadoopConfiguration)
    fileSystem.mkdirs(tempPath)

    val sourceArray = 0 until numInts
    val testRdd = spark.parallelize(sourceArray, numPartitions)

    // Executes a single test function and verifies that the resulting RDD contains the same
    // elements as testRdd (end-to-end correctness).
    def runTest(testFunc: (RDD[Int], String) => RDD[Int], path: String): Unit =
      assert(testFunc(testRdd, path).collect().sameElements(sourceArray))

    // Execute the test cases.
    tests.zipWithIndex.foreach { case (testFunc, testIndex) =>
      runTest(testFunc, s"$tempTestDir/$testIndex")
    }

    fileSystem.delete(tempPath, true)
    spark.stop()
    println("Tests successful!")
  }

  private def testSequenceFiles(rdd: RDD[Int], path: String): RDD[Int] = {
    println("Testing sequence files...")
    rdd.map(x => (x, x)).saveAsNewApiSequenceFile(path)
    // Sort the RDD because the partitions might be out of order.
    spark.sequenceFile[IntWritable, Text](path).map(_._1.get()).sortBy(x => x)
  }

  private def testTextFiles(rdd: RDD[Int], path: String): RDD[Int] = {
    println("Testing text files...")
    rdd.saveAsNewApiTextFile(path)
    // Sort the RDD because the partitions might be out of order.
    spark.textFile(path).map(_.toInt).sortBy(x => x)
  }
}

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

package org.apache.spark.rdd

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.storage.StorageLevel

class NewHadoopRddSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  before {
    // This is required because these tests use the SparkContext to create an RDD. Pass in false to
    // the SparkConf constructor so that the same configuration is loaded regardless of the system
    // properties.
    sc = new SparkContext("local", "test", new SparkConf(false))
  }

  /**
   * The purpose of these count() tests is to verify that the NewHadoopRDD class can correctly read
   * files made up of different numbers of partitions.
   */
  test("count(): small file") {
    testWriteAndRead("test_small", 1000)
  }

  test("count(): large file") {
    testWriteAndRead("test_large", 10000000)
  }

  test("count(): extra-large file") {
    testWriteAndRead("test_extra_large", 100000000)
  }

  test("collect(): reads correct data") {
    val numInts = 10000000
    val rdd = writeAndRead("test_collect", numInts)
    rdd.persist(StorageLevel.MEMORY_ONLY)
    assert(rdd.count() === numInts)
    assert((1 to rdd.count().toInt).map(_.toString) === rdd.collect())
  }

  /**
   * Writes a sequence of numInts integers to an HDFS file with the specified filename, then reads
   * the file using the new HDFS API and verifies that it contains the correct number of elements.
   */
  private def testWriteAndRead(filename: String, numInts: Int) = {
    assert(writeAndRead(filename, numInts).count() === numInts)
  }

  private def writeAndRead(filename: String, numInts: Int): RDD[String] = {
    val outputPath =
      SparkEnv.get.blockManager.blockFileManager.localDirs.values.head.toString() + "/" + filename
    sc.parallelize(1 to numInts).saveAsTextFile(outputPath)
    val formatClass = classOf[TextInputFormat]
    val keyClass = classOf[LongWritable]
    val valueClass = classOf[Text]
    sc.newAPIHadoopFile(outputPath, formatClass, keyClass, valueClass).map(_._2.toString)
  }
}

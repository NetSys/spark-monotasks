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

package org.apache.spark

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.storage.MultipleShuffleBlocksId

/** Test suite that runs all tests in ShuffleSuite, storing the shuffle data on-disk. */
class DiskShuffleSuite extends ShuffleSuite with BeforeAndAfterAll {
  override def beforeAll() {
    conf.set("spark.shuffle.writeToDisk", "true")
  }

  test("[SPARK-4085] rerun map stage if reduce stage cannot find its local shuffle file") {
    val myConf = conf.clone().set("spark.test.noStageRetry", "false")
    sc = new SparkContext("local", "test", myConf)
    val rdd = sc.parallelize(1 to 10, 2).map((_, 1)).reduceByKey(_ + _)
    rdd.count()

    // Delete one of the local shuffle blocks.
    val shuffleBlockId = MultipleShuffleBlocksId(0, 0)
    val blockStatus = sc.env.blockManager.getStatus(shuffleBlockId).getOrElse(
      fail(s"Expected shuffle block $shuffleBlockId to be stored in the blockManager"))
    val diskId = blockStatus.diskId.getOrElse(
      fail(s"Expected status for block $shuffleBlockId to include a disk id"))
    val shuffleFile =
      sc.env.blockManager.blockFileManager.getBlockFile(shuffleBlockId, diskId).getOrElse(
        fail(s"Expected blockFileManager to return the file where block $shuffleBlockId is " +
          "stored."))
    shuffleFile.delete()

    // This count should retry the execution of the previous stage and rerun shuffle, without
    // throwing an exception.
    rdd.count()
  }
}

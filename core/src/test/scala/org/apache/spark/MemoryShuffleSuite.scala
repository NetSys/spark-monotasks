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

import scala.concurrent.duration._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._

import org.apache.spark.storage.ShuffleBlockId

/** Test suite that runs all tests in ShuffleSuite, storing the shuffle data in-memory. */
class MemoryShuffleSuite extends ShuffleSuite with BeforeAndAfterAll {
  override def beforeAll() {
    conf.set("spark.shuffle.writeToDisk", "false")
  }

  test("Shuffle data is deleted immediately after stage finishes") {
    sc = new SparkContext("local", "test", conf)
    val rdd = sc.parallelize(1 to 10, 2).map((_, 1)).reduceByKey(_ + _)
    rdd.count()

    // Make sure shuffle data gets deleted.
    val shuffleBlockIds = List(
      ShuffleBlockId(0, 0, 0),
      ShuffleBlockId(0, 0, 1),
      ShuffleBlockId(0, 1, 0),
      ShuffleBlockId(0, 1, 1))
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      val foundBlockIdStatuses = shuffleBlockIds.flatMap(sc.env.blockManager.getStatus(_))
      assert(foundBlockIdStatuses.isEmpty)
    }
  }
}

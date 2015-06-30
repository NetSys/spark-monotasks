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

package org.apache.spark.monotasks

import scala.collection.mutable.HashSet

import org.mockito.Mockito.mock

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.SparkEnv

class MonotaskSuite extends FunSuite with Matchers {
  test("monotasks are assigned unique IDs") {
    // Setup a dummy SparkEnv to avoid a NPE when Monotask.scala tries to access it.
    SparkEnv.set(mock(classOf[SparkEnv]))

    val numMonotasks = 10
    val monotaskList = Array.fill[Monotask](numMonotasks)(new SimpleMonotask(0))

    // Make sure that all of the IDs are unique.
    val monotaskIdSet = new HashSet[Long]()
    monotaskList.foreach(monotaskIdSet += _.taskId)
    assert(numMonotasks === monotaskIdSet.size)
  }
}

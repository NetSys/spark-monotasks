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

import org.scalatest.{FunSuite, Matchers}

class MonotaskSuite extends FunSuite with Matchers {
 test("monotasks are assigned unique IDs") {
   val localDagScheduler = new LocalDagScheduler()
   val numMonotasks = 10
   val monotaskList = Array.fill[Monotask](numMonotasks)(new SimpleMonotask(localDagScheduler))

   // Make sure that all of the IDs are unique.
   val monotaskIdSet = new HashSet[Long]()
   monotaskList.foreach(monotaskIdSet += _.taskId)
   assert(numMonotasks === monotaskIdSet.size)
 }
}

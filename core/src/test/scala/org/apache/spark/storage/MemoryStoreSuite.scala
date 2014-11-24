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

package org.apache.spark.storage

import org.mockito.Mockito._

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.SparkConf

class MemoryStoreSuite extends FunSuite with BeforeAndAfterEach {
  var memoryStore: MemoryStore = _

  override def beforeEach() {
    val blockManager = mock(classOf[BlockManager])
    val sparkConf = mock(classOf[SparkConf])
    doReturn(sparkConf).when(blockManager).conf

    val maxMemory = 10000000
    memoryStore = new MemoryStore(blockManager, maxMemory)
  }

  test("putValue results in block getting stored") {
    val testValue = "This is a test value"
    val blockId = new MonotaskResultBlockId(5)
    memoryStore.putValue(blockId, testValue)

    val storedValue = memoryStore.getValue(blockId)
    assert(storedValue.isDefined)
    assert(testValue === storedValue.get.asInstanceOf[String])
  }

  test("getValue returns nothing when no block is stored") {
    assert(memoryStore.getValue(new MonotaskResultBlockId(1)).isEmpty)
  }
}

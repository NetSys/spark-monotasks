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

import java.nio.ByteBuffer

import org.mockito.Mockito.mock
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.SparkException

class MemoryStoreSuite extends FunSuite with BeforeAndAfterEach {
  private val blockId = new MonotaskResultBlockId(5)
  private val bufferSizeBytes = 10
  private val maxHeapMemory = 100L
  private val maxOffHeapMemory = 100L
  private var memoryStore: MemoryStore = _


  override def beforeEach() {
    val blockManager = mock(classOf[BlockManager])
    memoryStore = new MemoryStore(blockManager, maxHeapMemory, maxOffHeapMemory)
  }

  test("cacheBytes: caching a non-direct ByteBuffer decrements free heap memory") {
    val buffer = ByteBuffer.allocate(bufferSizeBytes)
    memoryStore.cacheBytes(blockId, buffer, deserialized = false)
    assert(memoryStore.freeHeapMemory === maxHeapMemory - bufferSizeBytes)
    assert(memoryStore.freeOffHeapMemory === maxOffHeapMemory)
  }

  test("cacheBytes: caching a direct ByteBuffer decrements free off-heap memory") {
    val buffer = ByteBuffer.allocateDirect(bufferSizeBytes)
    memoryStore.cacheBytes(blockId, buffer, deserialized = false)
    assert(memoryStore.freeHeapMemory === maxHeapMemory)
    assert(memoryStore.freeOffHeapMemory === maxOffHeapMemory - bufferSizeBytes)
  }

  test("cacheBytes: caching directByteBuffer larger than off-heap memory throws exception") {
    val buffer = ByteBuffer.allocateDirect((maxOffHeapMemory + 1).toInt)
    intercept[SparkException] {
      memoryStore.cacheBytes(blockId, buffer, deserialized = false)
    }
  }

  test("cacheBytes: caching directByteBuffers whose sum exceeds off-heap memory throws exception") {
    val buffer1 = ByteBuffer.allocateDirect(50)
    val buffer2 = ByteBuffer.allocateDirect((maxOffHeapMemory + 1 - 50).toInt)
    memoryStore.cacheBytes(blockId, buffer1, deserialized = false)
    intercept[SparkException] {
      memoryStore.cacheBytes(blockId, buffer2, deserialized = false)
    }
  }
}

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

package org.apache.spark.monotasks.compute

import java.nio.ByteBuffer

import org.mockito.Mockito.{mock, never, timeout, verify, when}

import org.scalatest.FunSuite

import org.apache.spark.TaskContextImpl
import org.apache.spark.storage.{BlockManager, MemoryStore, TestBlockId}

class ComputeSchedulerSuite extends FunSuite {
  test("monotasks aren't scheduled until memory is available") {
    // Create a MemoryStore and put enough data in it that it has no remaining space.
    val maxMemoryBytes = 300
    val memoryStore = new MemoryStore(
      mock(classOf[BlockManager]),
      maxMemoryBytes,
      targetMaxOffHeapMemory = 0)
    val dummyData = ByteBuffer.wrap((1 to maxMemoryBytes).map(_.toByte).toArray)
    val blockId = new TestBlockId("testData")
    memoryStore.cacheBytes(blockId, dummyData, false)

    val computeScheduler = new ComputeScheduler(threads = 1)
    computeScheduler.initialize(memoryStore)

    // Because there is no free memory, submitted monotasks shouldn't be run immediately.
    val mockComputeMonotask = mock(classOf[ComputeMonotask])
    when(mockComputeMonotask.context).thenReturn(new TaskContextImpl(0, 0))
    computeScheduler.submitTask(mockComputeMonotask)
    verify(mockComputeMonotask, never()).executeAndHandleExceptions()

    // Once memory becomes available in the memory store, the task should be run.
    memoryStore.remove(blockId)
    verify(mockComputeMonotask, timeout(1000)).executeAndHandleExceptions()
  }
}
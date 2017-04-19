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

import scala.concurrent.duration._
import scala.language.postfixOps

import org.mockito.Mockito.{mock, never, timeout, verify, when}

import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually.{eventually, interval => scalatestInterval,
  timeout => scalatestTimeout}

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
    val mockShuffleMapMonotask = mock(classOf[ShuffleMapMonotask[Any]])
    when(mockShuffleMapMonotask.context).thenReturn(new TaskContextImpl(0, 0))
    computeScheduler.submitTask(mockShuffleMapMonotask)
    verify(mockShuffleMapMonotask, never()).executeAndHandleExceptions()

    // Once memory becomes available in the memory store, the task should be run.
    memoryStore.remove(blockId)
    verify(mockShuffleMapMonotask, timeout(1000)).executeAndHandleExceptions()
  }

  /**
   * A ComputeMonotask that waits until its internal shouldFinish variable is set to true to
   * finish.
   */
  private class WaitingComputeMonotask(context: TaskContextImpl) extends ComputeMonotask(context) {
    @volatile var shouldFinish = false
    @volatile var isStarted = false

    override def executeAndHandleExceptions() = synchronized {
      isStarted = true
      while (!shouldFinish) {
        this.wait()
      }
    }

    /**
     * Don't do anything in execute, which never gets called since we overrode
     * executeAndHandleExceptions.
     */
    override def execute(): Option[ByteBuffer] = None
  }

  /** Ensures that PrepareMonotasks are run before other queued ComputeMonotasks. */
  test("prepare monotasks jump the queue") {
    // Make a memory store with available space.
    val maxMemoryBytes = 300
    val memoryStore = new MemoryStore(
      mock(classOf[BlockManager]),
      maxMemoryBytes,
      targetMaxOffHeapMemory = 0)
    val computeScheduler = new ComputeScheduler(threads = 2)
    computeScheduler.initialize(memoryStore)

    val taskContext = new TaskContextImpl(0, 0)

    // Submit three compute monotasks. The first two should be run, and the third should be
    // queued.
    val computeMonotask1 = new WaitingComputeMonotask(taskContext)
    val computeMonotask2 = new WaitingComputeMonotask(taskContext)
    val computeMonotask3 = new WaitingComputeMonotask(taskContext)

    computeScheduler.submitTask(computeMonotask1)
    computeScheduler.submitTask(computeMonotask2)
    computeScheduler.submitTask(computeMonotask3)

    // Wrap this in an eventually loop, since the compute threads pull tasks asynchronously.
    eventually(scalatestTimeout(3 seconds), scalatestInterval(10 milliseconds)) {
      assert(computeMonotask1.isStarted)
      assert(computeMonotask2.isStarted)
    }
    assert(!computeMonotask3.isStarted)

    // Now submit a prepare monotask, and finish one of the other compute monotasks. The prepare
    // monotask should run (and jump the queue in front of the other waiting compute monotask).
    // Because it's a mock, it will execute instantaneously, and then computeMonotask3 will start
    // (if computeMonotask3 was started first instead, the prepare monotask would never run,
    // because computeMonotask3 will keep running until shouldFinish is set to true).
    val mockPrepareMonotask = mock(classOf[PrepareMonotask])
    when(mockPrepareMonotask.context).thenReturn(taskContext)
    computeScheduler.submitTask(mockPrepareMonotask)

    // Finish a compute monotask.
    computeMonotask1.shouldFinish = true
    computeMonotask1.synchronized {
      computeMonotask1.notify()
    }

    verify(mockPrepareMonotask, timeout(1000)).executeAndHandleExceptions()
    assert(computeMonotask3.isStarted)
  }
}

/*
 * Copyright 2016 The Regents of The University California
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

package org.apache.spark.monotasks.network

import scala.concurrent.duration._
import scala.language.postfixOps

import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkConf, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.ShuffleBlockId

class NetworkSchedulerSuite extends FunSuite with BeforeAndAfterEach {

  private var localDagScheduler: LocalDagScheduler = _
  private var networkScheduler: NetworkScheduler = _
  private var currentNanos: Long = _
  private val elapsedMillis = 100L
  private val elapsedNanos = elapsedMillis * 1000000

  override def beforeEach(): Unit = {
    currentNanos = 0L

    // TaskMetrics requires access to the LocalDagScheduler via SparkEnv.
    val sparkEnv = mock(classOf[SparkEnv])
    localDagScheduler = mock(classOf[LocalDagScheduler])

    // Set the max concurrent tasks to 1, which simplifies testing of handling of task queues.
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.monotasks.network.maxConcurrentTasks", "1")

    networkScheduler = new NetworkScheduler(sparkConf)
    networkScheduler.initializeIdleTimeMeasurement(startNanos=0L)
    when(sparkEnv.localDagScheduler).thenReturn(localDagScheduler)
    SparkEnv.set(sparkEnv)
  }

  private def makeResponse(): NetworkResponseMonotask = {
    val response = mock(classOf[NetworkResponseMonotask])
    val context = mock(classOf[TaskContextImpl])
    val metrics = TaskMetrics.empty
    when(context.taskMetrics).thenReturn(metrics)
    when(response.context).thenReturn(context)
    response
  }

  private def updateAndVerifyMetricsOnResponseEnd(
      response: NetworkResponseMonotask,
      expectedNanos: Long): Unit = {
    when(localDagScheduler.getNetworkTransmitTotalIdleMillis(any())).thenReturn(
      networkScheduler.getTransmitTotalIdleMillis(currentNanos))
    val metrics = response.context.taskMetrics
    metrics.setMetricsOnTaskCompletion()
    assert(metrics.endNetworkTransmitTotalIdleMillis === expectedNanos)
  }

  test("transmit idle time counters updated properly for single response") {
    val response = makeResponse()

    // The NetworkScheduler is idle here for elapsedNanos.
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseStart(response, currentNanos)
    assert(networkScheduler.getTransmitTotalIdleMillis(currentNanos) === elapsedMillis)

    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseEnd(response, currentNanos)

    updateAndVerifyMetricsOnResponseEnd(response, elapsedMillis)
  }

  test("transmit idle time counters updated properly for non-overlapping responses") {
    val response1 = makeResponse()
    val response2 = makeResponse()

    // The NetworkScheduler is idle here for elapsedNanos.
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseStart(response1, currentNanos)
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseEnd(response1, currentNanos)

    updateAndVerifyMetricsOnResponseEnd(response2, elapsedMillis)

    // The NetworkScheduler is idle here for elapsedNanos.
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseStart(response2, currentNanos)
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseEnd(response2, currentNanos)

    updateAndVerifyMetricsOnResponseEnd(response2, 2 * elapsedMillis)
  }

  test("transmit idle time counters updated properly for overlapping responses") {
    val response1 = makeResponse()
    val response2 = makeResponse()

    // The NetworkScheduler is idle here for elapsedNanos.
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseStart(response1, currentNanos)
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseStart(response2, currentNanos)
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseEnd(response1, currentNanos)
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseEnd(response2, currentNanos)

    updateAndVerifyMetricsOnResponseEnd(response1, elapsedMillis)
    updateAndVerifyMetricsOnResponseEnd(response2, elapsedMillis)
  }

  test("correct idle time returned when queried when no NetworkResponseMonotasks are running") {
    val response = makeResponse()

    // The NetworkScheduler is idle here for elapsedNanos.
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseStart(response, currentNanos)
    currentNanos += elapsedNanos
    networkScheduler.updateIdleTimeOnResponseEnd(response, currentNanos)

    // The NetworkScheduler is idle here for elapsedNanos.
    currentNanos += elapsedNanos
    updateAndVerifyMetricsOnResponseEnd(response, 2 * elapsedMillis)
  }

  /**
   * A simple NetworkRequestMonotask implementation that records when execute was called. It's
   * necessary to have this class, rather than using a mock, so that the compareTo method in
   * NetworkRequestMonotask will get used when ordering which requests to run.
   */
  private class DummyNetworkRequestMonotask(taskId: Int, lowPriority: Boolean)
    extends NetworkRequestMonotask(
      new TaskContextImpl(taskId, 0),
      null,
      Seq((ShuffleBlockId(0, 0, taskId), 1)),
      lowPriority) {
    var hasExecuted = false

    override def execute(scheduler: NetworkScheduler): Unit = {
      hasExecuted = true
    }
  }

  test("high priority monotasks run before low priority ones") {
    val lowPriorityMonotask1 = new DummyNetworkRequestMonotask(taskId = 0, lowPriority = true)
    val lowPriorityMonotask2 = new DummyNetworkRequestMonotask(taskId = 1, lowPriority = true)
    val lowPriorityMonotask3 = new DummyNetworkRequestMonotask(taskId = 2, lowPriority = true)
    val highPriorityMonotask1 = new DummyNetworkRequestMonotask(taskId = 3, lowPriority = false)

    // Submit all of the low priority monotasks to the scheduler. Only the first one should be
    // launched.
    networkScheduler.submitTask(lowPriorityMonotask1)
    networkScheduler.submitTask(lowPriorityMonotask2)
    networkScheduler.submitTask(lowPriorityMonotask3)

    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(lowPriorityMonotask1.hasExecuted)
    }

    assert(!lowPriorityMonotask2.hasExecuted)
    assert(!lowPriorityMonotask3.hasExecuted)

    // Submit the high priority monotask. Nothing should happen yet, since there's currently a
    // monotask running.
    networkScheduler.submitTask(highPriorityMonotask1)

    assert(!highPriorityMonotask1.hasExecuted)

    // Finish the low priority monotask. This should cause the high priority monotask (and none of
    // the other monotasks) to be launched.
    networkScheduler.handleNetworkRequestSatisfied(lowPriorityMonotask1)

    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(highPriorityMonotask1.hasExecuted)
    }

    assert(!lowPriorityMonotask2.hasExecuted)
    assert(!lowPriorityMonotask3.hasExecuted)

    // When the high priority monotask finishes, the next low priority monotask should be launched.
    networkScheduler.handleNetworkRequestSatisfied(highPriorityMonotask1)
    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(lowPriorityMonotask2.hasExecuted)
    }
    assert(!lowPriorityMonotask3.hasExecuted)
  }

  test("monotasks with lowest reduce IDs run first") {
    // Create monotasks for 3 different macrotasks.
    val monotask1 = new DummyNetworkRequestMonotask(taskId = 1, lowPriority = true)
    val monotask2 = new DummyNetworkRequestMonotask(taskId = 2, lowPriority = true)
    val monotask3 = new DummyNetworkRequestMonotask(taskId = 3, lowPriority = true)

    // Submit all of the tasks to the network scheduler.
    networkScheduler.submitTask(monotask3)
    networkScheduler.submitTask(monotask2)
    networkScheduler.submitTask(monotask1)

    // Both of the monotasks for macrotask 1 should be launched, and no other moontasks.
    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(monotask3.hasExecuted)
    }
    assert(!monotask2.hasExecuted)
    assert(!monotask1.hasExecuted)

    // Finish the second monotask for 1. This should cause the monotasks for macrotask 2 to be
    // launched.
    networkScheduler.handleNetworkRequestSatisfied(monotask3)
    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(monotask1.hasExecuted)
    }
    assert(!monotask2.hasExecuted)
  }

  test("monotasks are launched if other monotasks for the same macrotask are already running") {
    // Create monotasks for 3 different macrotasks.
    val monotask1A = new DummyNetworkRequestMonotask(taskId = 1, lowPriority = true)
    val monotask1B = new DummyNetworkRequestMonotask(taskId = 1, lowPriority = true)
    val monotask2A = new DummyNetworkRequestMonotask(taskId = 2, lowPriority = true)
    val monotask2B = new DummyNetworkRequestMonotask(taskId = 2, lowPriority = true)
    val monotask3A = new DummyNetworkRequestMonotask(taskId = 3, lowPriority = true)
    val monotask3B = new DummyNetworkRequestMonotask(taskId = 3, lowPriority = true)

    // Submit all of the tasks to the network scheduler.
    networkScheduler.submitTask(monotask1A)
    networkScheduler.submitTask(monotask2A)
    networkScheduler.submitTask(monotask3A)
    networkScheduler.submitTask(monotask1B)
    networkScheduler.submitTask(monotask2B)
    networkScheduler.submitTask(monotask3B)

    // Both of the monotasks for macrotask 1 should be launched, and no other moontasks.
    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(monotask1A.hasExecuted)
      assert(monotask1B.hasExecuted)
    }
    assert(!monotask2A.hasExecuted)
    assert(!monotask2B.hasExecuted)
    assert(!monotask3A.hasExecuted)
    assert(!monotask3B.hasExecuted)

    // Finish one monotask for 1. This shouldn't cause any new ones to be launched.
    networkScheduler.handleNetworkRequestSatisfied(monotask1A)
    assert(!monotask2A.hasExecuted)
    assert(!monotask2B.hasExecuted)
    assert(!monotask3A.hasExecuted)
    assert(!monotask3B.hasExecuted)

    // Finish the second monotask for 1. This should cause the monotasks for macrotask 2 to be
    // launched.
    networkScheduler.handleNetworkRequestSatisfied(monotask1B)
    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(monotask2A.hasExecuted)
      assert(monotask2B.hasExecuted)
    }
    assert(!monotask3A.hasExecuted)
    assert(!monotask3B.hasExecuted)
  }

  test("monotasks with lowest reduceIds run first: more complex arrival pattern") {
    // Create monotasks for 3 different macrotasks.
    val monotask1A = new DummyNetworkRequestMonotask(taskId = 1, lowPriority = true)
    val monotask1B = new DummyNetworkRequestMonotask(taskId = 1, lowPriority = true)
    val monotask2A = new DummyNetworkRequestMonotask(taskId = 2, lowPriority = true)
    val monotask2B = new DummyNetworkRequestMonotask(taskId = 2, lowPriority = true)
    val monotask3A = new DummyNetworkRequestMonotask(taskId = 3, lowPriority = true)
    val monotask3B = new DummyNetworkRequestMonotask(taskId = 3, lowPriority = true)

    // Submit all of the tasks to the network scheduler. Submit just the A tasks.
    networkScheduler.submitTask(monotask1A)
    networkScheduler.submitTask(monotask2A)
    networkScheduler.submitTask(monotask3A)

    // The monotask for macrotask 1 should be launched, and no other monotasks.
    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(monotask1A.hasExecuted)
    }
    assert(!monotask2A.hasExecuted)
    assert(!monotask3A.hasExecuted)

    // Finish one monotask for 1. Now monotask 2A should be launched.
    networkScheduler.handleNetworkRequestSatisfied(monotask1A)
    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(monotask2A.hasExecuted)
    }
    assert(!monotask3A.hasExecuted)

    // Submit all of the B monotasks. Because there's a lower monotask in the queue (for macrotask
    // 1), monotask 2B shouldn't be started, even though there's currently another monotask running
    // for macrotask 2.
    networkScheduler.submitTask(monotask1B)
    networkScheduler.submitTask(monotask2B)
    networkScheduler.submitTask(monotask3B)

    assert(!monotask1B.hasExecuted)
    assert(!monotask2B.hasExecuted)
    assert(!monotask3A.hasExecuted)
    assert(!monotask3B.hasExecuted)

    // Finish 2A. Now, 1B should be launched.
    networkScheduler.handleNetworkRequestSatisfied(monotask2A)
    eventually(timeout(3 seconds), interval(10 milliseconds)) {
      assert(monotask1B.hasExecuted)
    }
    assert(!monotask2B.hasExecuted)
    assert(!monotask3A.hasExecuted)
    assert(!monotask3B.hasExecuted)
  }
}

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

import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.{SparkConf, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.monotasks.LocalDagScheduler

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
    networkScheduler = new NetworkScheduler(new SparkConf(false))
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
}

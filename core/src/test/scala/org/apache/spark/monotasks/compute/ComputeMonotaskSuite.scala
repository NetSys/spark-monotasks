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

import org.mockito.ArgumentMatcher
import org.mockito.Matchers.argThat
import org.mockito.Mockito.{mock, verify}

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.{DependencyManager, TaskMetrics}
import org.apache.spark.monotasks.{LocalDagScheduler, TaskFailure, TaskSuccess}

class ComputeMonotaskSuite extends FunSuite with BeforeAndAfterEach with LocalSparkContext {

  var localDagScheduler: LocalDagScheduler = _
  var taskContext: TaskContextImpl = _
  val taskMetrics = new TaskMetrics()

  override def beforeEach() {
    // Create a new SparkContext so that SparkEnv gets properly initialized.
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    localDagScheduler = mock(classOf[LocalDagScheduler])
    val dependencyManager = new DependencyManager(SparkEnv.get, conf, Nil, true)
    taskContext = new TaskContextImpl(
      SparkEnv.get, localDagScheduler, 500, dependencyManager, 12, 0)
    taskContext.initialize(0, 0)
  }

  test("executeAndHandleExceptions handles exceptions and notifies LocalDagScheduler of failure") {
    val monotask = new ComputeMonotask(taskContext) {
      override def execute(): Option[ByteBuffer] = throw new Exception("task failed")
    }

    monotask.executeAndHandleExceptions()

    // When an exception is thrown, the execute() method should still notify the LocalDagScheduler
    // that the task has failed.

    // Create a custom matcher that ensures that the task failure included the monotask created
    // above and that the failure reason is non-null.
    class TaskFailureContainsMonotask extends ArgumentMatcher[TaskFailure] {
      override def matches(o: Object): Boolean = o match {
        case failure: TaskFailure =>
          (failure.failedMonotask == monotask) && (failure.serializedFailureReason != null)
        case _ =>
          false
      }
    }
    verify(localDagScheduler).post(argThat(new TaskFailureContainsMonotask))
  }

  test("executeAndHandleExceptions notifies LocalDagScheduler of success") {
    val result = Some(ByteBuffer.allocate(2))
    val monotask = new ComputeMonotask(taskContext) {
      override def execute(): Option[ByteBuffer] = result
    }

    monotask.executeAndHandleExceptions()
    verify(localDagScheduler).post(TaskSuccess(monotask, result))
  }
}

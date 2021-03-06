/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import org.mockito.Matchers._
import org.mockito.Mockito._

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{TaskCompletionListenerException, TaskCompletionListener}

class TaskContextSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  test("executeOnCompleteCallbacks executed on task completion") {
    var callbackExecuted = false
    val taskContext = new TaskContextImpl(0, 0)
    taskContext.addTaskCompletionListener(context => callbackExecuted = true)
    taskContext.markTaskCompleted()
    assert(callbackExecuted)
  }

  test("all TaskCompletionListeners should be called even if some fail") {
    val context = new TaskContextImpl(0, 0)
    val listener = mock(classOf[TaskCompletionListener])
    context.addTaskCompletionListener(_ => throw new Exception("blah"))
    context.addTaskCompletionListener(listener)
    context.addTaskCompletionListener(_ => throw new Exception("blah"))

    intercept[TaskCompletionListenerException] {
      context.markTaskCompleted()
    }

    verify(listener, times(1)).onTaskCompletion(any())
  }

  test("TaskContext.attemptNumber should return attempt number, not task id (SPARK-4014)") {
    sc = new SparkContext("local[1,2]", "test")  // use maxRetries = 2 because we test failed tasks
    // Check that attemptIds are 0 for all tasks' initial attempts
    val attemptIds = sc.parallelize(Seq(1, 2), 2).mapPartitions { iter =>
        Seq(TaskContext.get().attemptNumber).iterator
      }.collect()
    assert(attemptIds.toSet === Set(0))

    // Test a job with failed tasks
    val attemptIdsWithFailedTask = sc.parallelize(Seq(1, 2), 2).mapPartitions { iter =>
      val attemptId = TaskContext.get().attemptNumber
      if (iter.next() == 1 && attemptId == 0) {
        throw new Exception("First execution of task failed")
      }
      Seq(attemptId).iterator
    }.collect()
    assert(attemptIdsWithFailedTask.toSet === Set(0, 1))
  }

  test("TaskContext.attemptId returns taskAttemptId for backwards-compatibility (SPARK-4014)") {
    sc = new SparkContext("local", "test")
    val attemptIds = sc.parallelize(Seq(1, 2, 3, 4), 4).mapPartitions { iter =>
      Seq(TaskContext.get().attemptId).iterator
    }.collect()
    assert(attemptIds.toSet === Set(0, 1, 2, 3))
  }
}

private object TaskContextSuite {
  @volatile var completed = false
}

private case class StubPartition(index: Int) extends Partition

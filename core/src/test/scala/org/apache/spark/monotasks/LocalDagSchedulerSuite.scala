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

import java.nio.ByteBuffer

import scala.collection.mutable.HashSet

import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito.{mock, never, verify, when}

import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

import org.apache.spark._
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.monotasks.compute.ComputeMonotask
import org.apache.spark.monotasks.disk.DiskMonotask
import org.apache.spark.monotasks.network.NetworkMonotask
import scala.Some

class LocalDagSchedulerSuite extends FunSuite with BeforeAndAfterEach with LocalSparkContext
  with Matchers {

  private var executorBackend: ExecutorBackend = _
  private var localDagScheduler: LocalDagScheduler = _

  override def beforeEach() {
    /* This is required because the LocalDagScheduler takes as input a BlockManager, which is
     * obtained from SparkEnv. Pass in false to the SparkConf constructor so that the same
     * configuration is loaded regardless of the system properties. */
    sc = new SparkContext("local", "test", new SparkConf(false))
    executorBackend = mock(classOf[ExecutorBackend])
    localDagScheduler = new LocalDagScheduler(executorBackend, SparkEnv.get.blockManager)
  }

  test("submitMonotasks: tasks with no dependencies are run immediately") {
    val noDependencyMonotask = new SimpleMonotask(0)
    localDagScheduler.submitMonotasks(List(noDependencyMonotask))

    assert(localDagScheduler.waitingMonotasks.isEmpty)
    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(noDependencyMonotask.taskId))
  }

  test("submitMonotasks: tasks with unsatisfied dependencies are not run immediately") {
    val firstMonotask = new SimpleMonotask(0)
    val secondMonotask = new SimpleMonotask(0)
    secondMonotask.addDependency(firstMonotask)
    localDagScheduler.submitMonotasks(List(firstMonotask, secondMonotask))

    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(firstMonotask.taskId))
    assert(1 === localDagScheduler.waitingMonotasks.size)
    assert(localDagScheduler.waitingMonotasks.contains(secondMonotask.taskId))
  }

  test("handleTaskCompletion results in appropriate new monotasks being run") {
    val firstMonotask = new SimpleMonotask(0)
    val secondMonotask = new SimpleMonotask(0)
    secondMonotask.addDependency(firstMonotask)
    localDagScheduler.submitMonotasks(List(firstMonotask, secondMonotask))
    localDagScheduler.handleTaskCompletion(firstMonotask)

    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(secondMonotask.taskId))
    assert(localDagScheduler.waitingMonotasks.isEmpty)
  }

  /**
   * Tests that when a serialized task result is provided to handleTaskCompletion(), the
   * LocalDagScheduler removes the associated macrotask from the set of running macrotasks, marks
   * the associated TaskContext as completed, and updates the executor backend that the task has
   * finished.
   */
  test("handleTaskCompletion handles serializedTaskResults properly") {
    val taskAttemptId = 0L
    val firstMonotask = new SimpleMonotask(taskAttemptId)
    val secondMonotask = new SimpleMonotask(taskAttemptId)
    secondMonotask.addDependency(firstMonotask)
    localDagScheduler.submitMonotasks(List(firstMonotask, secondMonotask))

    assert(localDagScheduler.runningMacrotaskAttemptIds.contains(taskAttemptId),
      (s"Task attempt id $taskAttemptId should have been added to the set of running ids when " +
        "the task was submitted"))

    localDagScheduler.handleTaskCompletion(firstMonotask)
    assert(localDagScheduler.runningMacrotaskAttemptIds.contains(taskAttemptId),
      (s"Task attempt id $taskAttemptId should still be in the set of running ids because no " +
        "task result was submitted, implying more monotasks for the macrotask are still running"))

    val result = ByteBuffer.allocate(2)
    localDagScheduler.handleTaskCompletion(secondMonotask, Some(result))
    assert(secondMonotask.context.isCompleted)
    verify(executorBackend).statusUpdate(meq(taskAttemptId), meq(TaskState.FINISHED), meq(result))
    assert(localDagScheduler.runningMacrotaskAttemptIds.isEmpty,
      s"Task attempt id $taskAttemptId should have been removed from running ids")
  }

  /**
   * Tests a more complicated case where monotasks form the following DAG (where E depends on
   * C and D, and C depends on A and B).
   *
   *     A --,
   *          >-- C --,
   *     B --'         >-- E
   *              D --'
   */
  test("handleTaskCompletion properly handles complex DAGs") {
    val monotaskA = new SimpleMonotask(0)
    val monotaskB = new SimpleMonotask(0)
    val monotaskC = new SimpleMonotask(0)
    val monotaskD = new SimpleMonotask(0)
    val monotaskE = new SimpleMonotask(0)

    monotaskC.addDependency(monotaskA)
    monotaskC.addDependency(monotaskB)
    monotaskE.addDependency(monotaskC)
    monotaskE.addDependency(monotaskD)

    localDagScheduler.submitMonotasks(List(monotaskA, monotaskB, monotaskC, monotaskD, monotaskE))

    // At first, tasks A, B, and D should be running.
    assert(3 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(monotaskA.taskId))
    assert(localDagScheduler.runningMonotasks.contains(monotaskB.taskId))
    assert(localDagScheduler.runningMonotasks.contains(monotaskD.taskId))
    assert(2 === localDagScheduler.waitingMonotasks.size)

    // When D finishes, no new tasks should be run, because E still depends on task C.
    localDagScheduler.handleTaskCompletion(monotaskD)
    assert(2 === localDagScheduler.runningMonotasks.size)
    assert(2 === localDagScheduler.waitingMonotasks.size)

    // Similarly, when B finishes, no new tasks should be run, because C still depends on A.
    localDagScheduler.handleTaskCompletion(monotaskB)
    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(2 === localDagScheduler.waitingMonotasks.size)

    // When A finishes, C should be run.
    localDagScheduler.handleTaskCompletion(monotaskA)
    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(monotaskC.taskId))
    assert(1 === localDagScheduler.waitingMonotasks.size)
    assert(localDagScheduler.waitingMonotasks.contains(monotaskE.taskId))

    // Finally, when C finishes, E can be run.
    localDagScheduler.handleTaskCompletion(monotaskC)
    assert(localDagScheduler.waitingMonotasks.isEmpty)
    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(monotaskE.taskId))

    // Make a dummy result to pass in as part of the task completion to signal that the macrotask
    // has completed.
    val result = ByteBuffer.allocate(2)
    localDagScheduler.handleTaskCompletion(monotaskE, Some(result))
    assertDataStructuresEmpty()
  }

   /**
    * Creates a dag of monotasks:
    *
    *     A --,
    *           >-- C
    *     B --'
    *
    * and ensures that, when monotask A fails, C is removed from the list of waiting monotasks and
    * that monotasks for other macrotasks are not affected.
    */
  test("handleTaskFailure removes dependent monotasks from waiting monotasks") {
    val taskAttemptId = 12L
    val monotaskA = new SimpleMonotask(taskAttemptId)
    val monotaskB = new SimpleMonotask(taskAttemptId)
    val monotaskC = new SimpleMonotask(taskAttemptId)

    monotaskC.addDependency(monotaskA)
    monotaskC.addDependency(monotaskB)

    // Submit the monotasks to run, and then fail monotask A. C should be removed from the list of
    // waiting monotasks (since it can never be run once A fails).
    localDagScheduler.submitMonotasks(List(monotaskA, monotaskB, monotaskC))

    // Also submit two monotasks for a separate task attempt, to make sure it is not failed.
    val taskAttemptId2 = 15L
    val firstMonotask = new SimpleMonotask(taskAttemptId2)
    val secondMonotask = new SimpleMonotask(taskAttemptId2)
    secondMonotask.addDependency(firstMonotask)
    localDagScheduler.submitMonotasks(List(firstMonotask, secondMonotask))

    localDagScheduler.handleTaskFailure(monotaskA, ByteBuffer.allocate(12))
    val waitingErrorMessage = ("The only remaining waiting monotask should be the monotask from " +
      s"task attempt $taskAttemptId2")
    assert(Set(secondMonotask.taskId) === localDagScheduler.waitingMonotasks, waitingErrorMessage)

    assert(!localDagScheduler.runningMonotasks.contains(monotaskA.taskId),
      "The failed monotask should have been removed from the running monotasks")

    assert(Set(monotaskB.taskId, firstMonotask.taskId) === localDagScheduler.runningMonotasks,
      "Running monotasks should not be affected by the failure")
  }

  /**
   * Creates a DAG of monotasks for the same macrotask:
   *
   *     A --,
   *          >-- C
   *     B --'
   *
   *  and ensures that only the first monotask failure triggers a notification to
   *  executorBackend that a task failed.
   */
  test("handleTaskFailure notifies executorBackend on first failure") {
    val taskAttemptId = 30L
    val monotaskA = new SimpleMonotask(taskAttemptId)
    val monotaskB = new SimpleMonotask(taskAttemptId)
    val monotaskC = new SimpleMonotask(taskAttemptId)

    monotaskC.addDependency(monotaskA)
    monotaskC.addDependency(monotaskB)

    localDagScheduler.submitMonotasks(List(monotaskA, monotaskB, monotaskC))

    // The first monotask failure should result in both marking the TaskContext as completed and
    // sending a status update to the executor backend.
    val failureReason = ByteBuffer.allocate(20)
    localDagScheduler.handleTaskFailure(monotaskB, failureReason)
    assert(monotaskB.context.isCompleted)
    verify(executorBackend).statusUpdate(
      meq(taskAttemptId), meq(TaskState.FAILED), meq(failureReason))

    // Another failure for the same macrotask should not trigger another status update (so including
    // the earlier time statusUpdate() was called, the total invocation count should be 1).
    localDagScheduler.handleTaskFailure(monotaskA, ByteBuffer.allocate(1))
    verify(executorBackend).statusUpdate(any(), any(), any())
  }

  /**
   * Creates a DAG of monotasks for the same macrotask:
   *
   *      ,-- B
   * A --<
   *      `-- C --,
   *               >-- E
   *          D --'
   *
   * and tests that if D fails while A is executing, then when A completes it removes B and C from
   * the list of waiting monotasks.
   */
  test("handleTaskFailure: fails all of a macrotask's waiting monotasks") {
    val taskAttemptId = 42L
    val monotaskA = new SimpleMonotask(taskAttemptId)
    val monotaskB = new SimpleMonotask(taskAttemptId)
    val monotaskC = new SimpleMonotask(taskAttemptId)
    val monotaskD = new SimpleMonotask(taskAttemptId)
    val monotaskE = new SimpleMonotask(taskAttemptId)

    monotaskB.addDependency(monotaskA)
    monotaskC.addDependency(monotaskA)
    monotaskE.addDependency(monotaskC)
    monotaskE.addDependency(monotaskD)

    localDagScheduler.submitMonotasks(List(monotaskA, monotaskB, monotaskC, monotaskD, monotaskE))

    // Create a dummy failure reason.
    val failureReason = ByteBuffer.allocate(2)
    localDagScheduler.handleTaskFailure(monotaskD, failureReason)

    // Verify that D removed E from the list of waiting monotask and removed the macrotask from
    // macrotaskRemainingMonotasks.
    assert(!localDagScheduler.waitingMonotasks.contains(monotaskE.taskId))
    assert(!localDagScheduler.runningMacrotaskAttemptIds.contains(taskAttemptId))

    // Suppose that A completed normally, uneffected by D's failure.
    localDagScheduler.handleTaskCompletion(monotaskA)

    // Make sure that A removed B and C from the list of waiting monotasks.
    assertDataStructuresEmpty()
  }

  /**
   * This test ensures that LocalDagScheduler returns the correct number of macrotasks that are
   * doing computation, network, or disk.  It constructs two macrotasks:
   *
   * Macrotask 0 has three monotasks: first a network monotask, then a compute monotask, then a disk
   * monotask.
   *
   * Macrotask 1 has two monotasks: first a disk monotask, and then a compute monotask.
   *
   * This test ensures that as those monotasks start and finish, the counts of the number of
   * macrotasks using each resource are correct.
   */
  test("updateMetricsForStartedMonotask and updateMetricsForFinishedMonotask") {
    // Setup the 3 monotasks for macrotask 0.
    val macrotask0Context = new TaskContextImpl(null, localDagScheduler, 0, null, 0, 0)
    val macrotask0NetworkMonotask = mock(classOf[NetworkMonotask])
    when(macrotask0NetworkMonotask.dependencies).thenReturn(HashSet.empty[Long])
    when(macrotask0NetworkMonotask.context).thenReturn(macrotask0Context)
    val macrotask0ComputeMonotask = mock(classOf[ComputeMonotask])
    when(macrotask0ComputeMonotask.dependencies).thenReturn(HashSet.empty[Long])
    when(macrotask0ComputeMonotask.context).thenReturn(macrotask0Context)
    val macrotask0DiskMonotask = mock(classOf[DiskMonotask])
    when(macrotask0DiskMonotask.dependencies).thenReturn(HashSet.empty[Long])
    when(macrotask0DiskMonotask.context).thenReturn(macrotask0Context)

    // Setup the 2 monotasks for macrotask 1.
    val macrotask1Context = new TaskContextImpl(null, localDagScheduler, 0, null, 1, 0)
    val macrotask1DiskMonotask = mock(classOf[DiskMonotask])
    when(macrotask1DiskMonotask.dependencies).thenReturn(HashSet.empty[Long])
    when(macrotask1DiskMonotask.context).thenReturn(macrotask1Context)
    val macrotask1ComputeMonotask = mock(classOf[ComputeMonotask])
    when(macrotask1ComputeMonotask.dependencies).thenReturn(HashSet.empty[Long])
    when(macrotask1ComputeMonotask.context).thenReturn(macrotask1Context)

    localDagScheduler.updateMetricsForStartedMonotask(macrotask0NetworkMonotask)
    assert(localDagScheduler.getNumMacrotasksInCompute() === 0)
    assert(localDagScheduler.getNumMacrotasksInDisk() === 0)
    assert(localDagScheduler.getNumMacrotasksInNetwork() === 1)

    localDagScheduler.updateMetricsForStartedMonotask(macrotask1DiskMonotask)
    assert(localDagScheduler.getNumMacrotasksInCompute() === 0)
    assert(localDagScheduler.getNumMacrotasksInDisk() === 1)
    assert(localDagScheduler.getNumMacrotasksInNetwork() === 1)

    localDagScheduler.updateMetricsForFinishedMonotask(macrotask0NetworkMonotask)
    localDagScheduler.updateMetricsForStartedMonotask(macrotask0ComputeMonotask)
    assert(localDagScheduler.getNumMacrotasksInCompute() === 1)
    assert(localDagScheduler.getNumMacrotasksInDisk() === 1)
    assert(localDagScheduler.getNumMacrotasksInNetwork() === 0)

    localDagScheduler.updateMetricsForFinishedMonotask(macrotask1DiskMonotask)
    localDagScheduler.updateMetricsForStartedMonotask(macrotask1ComputeMonotask)
    assert(localDagScheduler.getNumMacrotasksInCompute() === 2)
    assert(localDagScheduler.getNumMacrotasksInDisk() === 0)
    assert(localDagScheduler.getNumMacrotasksInNetwork() === 0)

    localDagScheduler.updateMetricsForFinishedMonotask(macrotask0ComputeMonotask)
    localDagScheduler.updateMetricsForStartedMonotask(macrotask0DiskMonotask)
    assert(localDagScheduler.getNumMacrotasksInCompute() === 1)
    assert(localDagScheduler.getNumMacrotasksInDisk() === 1)
    assert(localDagScheduler.getNumMacrotasksInNetwork() === 0)

    localDagScheduler.updateMetricsForFinishedMonotask(macrotask0DiskMonotask)
    assert(localDagScheduler.getNumMacrotasksInCompute() === 1)
    assert(localDagScheduler.getNumMacrotasksInDisk() === 0)
    assert(localDagScheduler.getNumMacrotasksInNetwork() === 0)
  }

  test("waitUntilAllTasksComplete returns immediately when no tasks are running or waiting") {
    assert(localDagScheduler.waitUntilAllTasksComplete(0))
  }

  test("waitUntilAllTasksComplete waits for all tasks to complete") {
    // Create a simple DAG with two tasks.
    val firstMonotask = new SimpleMonotask(0)
    val secondMonotask = new SimpleMonotask(0)
    secondMonotask.addDependency(firstMonotask)
    localDagScheduler.submitMonotasks(List(firstMonotask, secondMonotask))

    assert(!localDagScheduler.waitUntilAllTasksComplete(10))

    localDagScheduler.handleTaskCompletion(firstMonotask)
    assert(!localDagScheduler.waitUntilAllTasksComplete(10))

    // Make a dummy result to pass in as part of the task completion to signal that the macrotask
    // has completed.
    val result = ByteBuffer.allocate(2)
    localDagScheduler.handleTaskCompletion(secondMonotask, Some(result))
    assert(localDagScheduler.waitUntilAllTasksComplete(10))
    assertDataStructuresEmpty()
  }

  /**
   * Ensures that all of the data structures in the LocalDagScheduler are empty after all
   * tasks have finished.
   */
  private def assertDataStructuresEmpty() = {
    assert(localDagScheduler.waitingMonotasks.isEmpty)
    assert(localDagScheduler.runningMonotasks.isEmpty)
    assert(localDagScheduler.runningMacrotaskAttemptIds.isEmpty)
  }
}

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

import org.apache.spark.{Logging, TaskState}
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.monotasks.compute.{ComputeMonotask, ComputeScheduler}
import org.apache.spark.monotasks.disk.{DiskMonotask, DiskScheduler}
import org.apache.spark.monotasks.network.{NetworkMonotask, NetworkScheduler}
import org.apache.spark.storage.BlockManager

/**
 * LocalDagScheduler tracks running and waiting monotasks. When all of a monotask's
 * dependencies have finished executing, the LocalDagScheduler will submit the monotask
 * to the appropriate scheduler to be executed once sufficient resources are available.
 *
 * TODO: The LocalDagScheduler should implement thread safety using an actor or event loop, rather
 *       than having all methods be synchronized (which can lead to monotasks that block waiting
 *       for the LocalDagScheduler).
 */
private[spark] class LocalDagScheduler(
    executorBackend: ExecutorBackend,
    val blockManager: BlockManager)
  extends Logging {

  val computeScheduler = new ComputeScheduler
  val networkScheduler = new NetworkScheduler
  val diskScheduler = new DiskScheduler(blockManager)

  /* IDs of monotasks that are waiting for dependencies to be satisfied. This exists solely for
   * debugging/testing and is not needed for maintaining correctness. */
  val waitingMonotasks = new HashSet[Long]()

  /* IDs of monotasks that have been submitted to a scheduler to be run. This exists solely for
   * debugging/testing and is not needed for maintaining correctness. */
  val runningMonotasks = new HashSet[Long]()

  /* IDs for macrotasks that currently are running. Used to determine whether to notify the
   * executor backend that a task has failed (used to avoid duplicate failure messages if multiple
   * monotasks for the macrotask fail). */
  val runningMacrotaskAttemptIds = new HashSet[Long]()

  def submitMonotask(monotask: Monotask) = synchronized {
    if (monotask.dependencies.isEmpty) {
      scheduleMonotask(monotask)
    } else {
      waitingMonotasks += monotask.taskId
    }
    val taskAttemptId = monotask.context.taskAttemptId
    logDebug(s"Submitting monotask $monotask (id: ${monotask.taskId} for macrotask $taskAttemptId")
    runningMacrotaskAttemptIds += taskAttemptId
  }

  def submitMonotasks(monotasks: Seq[Monotask]) = synchronized {
    monotasks.foreach(submitMonotask(_))
  }

  /**
   * Marks the monotask as successfully completed by updating the dependency tree and running any
   * newly runnable monotasks.
   *
   * @param completedMonotask The monotask that has completed.
   * @param serializedTaskResult If the monotask was the final monotask for the macrotask, a
   *                             serialized TaskResult to be sent to the driver (None otherwise).
   */
  def handleTaskCompletion(
      completedMonotask: Monotask,
      serializedTaskResult: Option[ByteBuffer] = None) = synchronized {
    logDebug(s"Monotask $completedMonotask (id: ${completedMonotask.taskId}) for " +
      s"macrotask ${completedMonotask.context.taskAttemptId} has completed")
    completedMonotask.dependents.foreach { monotask =>
      monotask.dependencies -= completedMonotask.taskId
      if (monotask.dependencies.isEmpty) {
        assert(
          waitingMonotasks.contains(monotask.taskId),
          "Monotask dependencies should only include tasks that have not yet run")
        scheduleMonotask(monotask)
      }
    }
    runningMonotasks.remove(completedMonotask.taskId)

    serializedTaskResult.map { result =>
      val taskAttemptId = completedMonotask.context.taskAttemptId
      // Tell the executorBackend that the macrotask finished, if we haven't already.
      if (runningMacrotaskAttemptIds.remove(taskAttemptId)) {
        completedMonotask.context.markTaskCompleted()
        logDebug(s"Notfiying executorBackend about successful completion of task $taskAttemptId")
        executorBackend.statusUpdate(taskAttemptId, TaskState.FINISHED, result)
      } else {
        // This should never happen because we remove ids from runningMacrotaskAttemptIds only
        // when tasks complete successfully (which only happens once per task) or if they fail,
        // in which case the task should not also be able to complete successfully.
        assert(false,
          s"Task $taskAttemptId finished multiple times. Possible bug in dependency tracking!")
      }
    }
  }

  /**
   * Marks the monotask and all monotasks that depend on it as failed and notifies the executor
   * backend that the associated macrotask has failed.
   *
   * @param failedMonotask The monotask that failed.
   * @param serializedFailureReason A serialized TaskFailedReason describing why the task failed.
   */
  def handleTaskFailure(failedMonotask: Monotask, serializedFailureReason: ByteBuffer)
    = synchronized {
    logInfo(s"Monotask ${failedMonotask.taskId} (for macrotask " +
      s"${failedMonotask.context.taskAttemptId}) failed")
    runningMonotasks -= failedMonotask.taskId
    failDependentMonotasks(failedMonotask, failedMonotask.taskId)
    val taskAttemptId = failedMonotask.context.taskAttemptId

    // Notify the executor backend that the macrotask has failed, if we didn't already.
    if (runningMacrotaskAttemptIds.remove(taskAttemptId)) {
      failedMonotask.context.markTaskCompleted()
      executorBackend.statusUpdate(taskAttemptId, TaskState.FAILED, serializedFailureReason)
    }
  }

  private def failDependentMonotasks(monotask: Monotask, originalFailedTaskId: Long) {
    // TODO: Right now, this may still result in some unnecessary monotasks being run. For example,
    //       given the following dependency tree:
    //
    //       A --> B --> C
    //                   ^
    //                   |
    //                   D
    //
    //       If D fails, C will never be run (because it will forever have an unsatisfied dependency
    //       on D).  But, if A is running when D fails, B will still be run (which is wasted
    //       effort).
    //
    //       We could fix this problem by cleaning up the dependencies / dependents when a task
    //       fails to remove tasks that have failed or should never run due to a failure.
    //       Alternately, we could set an "isZombie" flag in the monotask to signal to schedulers
    //       not to run it.
    //
    //       We also don't interrupt monotasks that are already running.
    //
    //       https://github.com/NetSys/spark-monotasks/issues/10
    //
    monotask.dependents.foreach { dependentMonotask =>
      logDebug(s"Failing monotask ${dependentMonotask.taskId} because it dependended on monotask " +
        s"$originalFailedTaskId, which failed")
      waitingMonotasks -= dependentMonotask.taskId
      failDependentMonotasks(dependentMonotask, originalFailedTaskId)
    }
  }

  /**
   * Submits a monotask to the relevant scheduler to be executed. This method should only be called
   * after all of the monotask's dependencies have been satisfied.
   */
  private def scheduleMonotask(monotask: Monotask) {
    assert(monotask.dependencies.isEmpty)
    monotask match {
      case computeMonotask: ComputeMonotask => computeScheduler.submitTask(computeMonotask)
      case networkMonotask: NetworkMonotask => networkScheduler.submitTask(networkMonotask)
      case diskMonotask: DiskMonotask => diskScheduler.submitTask(diskMonotask)
      case _ => logError(s"Received unexpected type of monotask: $monotask")
    }
    /* Add the monotask to runningMonotasks before removing it from waitingMonotasks to avoid
     * a race condition in waitUntilAllTasksComplete where both sets are empty. */
    runningMonotasks += monotask.taskId
    waitingMonotasks.remove(monotask.taskId)
  }

  /**
   * For testing only. Waits until all monotasks have completed, or until the specified time has
   * elapsed. Returns true if all monotasks have completed and false if the specified amount of time
   * elapsed before all monotasks completed.
   */
  def waitUntilAllTasksComplete(timeoutMillis: Int): Boolean = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!this.synchronized(waitingMonotasks.isEmpty && runningMonotasks.isEmpty)) {
      if (System.currentTimeMillis > finishTime) {
        return false
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and wait/notify
       * add overhead in the general case. */
      Thread.sleep(10)
    }
    true
  }
}

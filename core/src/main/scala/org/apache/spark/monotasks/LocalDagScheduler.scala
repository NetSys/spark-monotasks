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

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.{Logging, SparkConf, TaskContextImpl, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.monotasks.compute.{ComputeMonotask, ComputeScheduler,
  ResultSerializationMonotask}
import org.apache.spark.monotasks.disk.{DiskMonotask, DiskScheduler}
import org.apache.spark.monotasks.network.{NetworkMonotask, NetworkScheduler}
import org.apache.spark.storage.{BlockFileManager, MemoryStore}
import org.apache.spark.util.{EventLoop, SparkUncaughtExceptionHandler}

/**
 * LocalDagScheduler tracks running and waiting monotasks. When all of a monotask's
 * dependencies have finished executing, the LocalDagScheduler will submit the monotask
 * to the appropriate scheduler to be executed once sufficient resources are available.
 *
 * In general, external classes should interact with LocalDagScheduler by calling post() with
 * a LocalDagSchedulerEvent.  All events are processed asynchronously via a single-threaded event
 * loop.  The only exception to this is monitoring functions (to get the number of running tasks,
 * for example), which are not processed by the event loop.
 */
private[spark] class LocalDagScheduler(blockFileManager: BlockFileManager, conf: SparkConf)
  extends EventLoop[LocalDagSchedulerEvent]("local-dag-scheduler-event-loop") with Logging {

  /**
   * TaskContextImpl to use for monotasks that do not correspond to a macrotask running on this
   * machine (e.g., DiskRemoveMonotasks that are removing shuffle data that is no longer needed).
   */
  val genericTaskContext = new TaskContextImpl(-1, 0, taskIsRunningRemotely = true)

  /**
   * Backend to send notifications to when macrotasks complete successfully. Set by
   * setExecutorBackend().
   */
  private var executorBackend: Option[ExecutorBackend] = None

  private val computeScheduler = new ComputeScheduler
  private val networkScheduler = new NetworkScheduler
  private val diskScheduler = new DiskScheduler(blockFileManager, conf)

  /** Monotasks that are waiting for their dependencies to be satisfied. */
  private[monotasks] val waitingMonotasks = new HashSet[Monotask]()

  /**
   * IDs of monotasks that have been submitted to a scheduler to be run. This exists solely for
   * debugging/testing and is not needed for maintaining correctness.
   */
  private[monotasks] val runningMonotasks = new HashSet[Long]()

  /**
   * IDs for macrotasks that currently are running. Used to determine whether to notify the
   * executor backend that a task has failed (used to avoid duplicate failure messages if multiple
   * monotasks for the macrotask fail).
   */
  private[monotasks] val runningMacrotaskAttemptIds = new HashSet[Long]()

  /**
   * For each remote macrotask that has monotasks running on this executor, the number of remaining
   * monotasks to be run (including any monotasks that are currently running). This is used to
   * clean up state when all of the monotasks corresponding to a remote macrotask have completed.
   */
  private val remoteMacrotaskAttemptIdToRemainingMonotasks = new HashMap[Long, Int]()

  // These maps each map a macrotask ID to the number of a particular type of monotasks that
  // are running for that macrotask. Each map includes only macrotasks that have at least one
  // running monotask of the relevant type.
  private val macrotaskIdToNumRunningComputeMonotasks = new HashMap[Long, Int]()
  private val macrotaskIdToNumRunningDiskMonotasks = new HashMap[Long, Int]()
  private val macrotaskIdToNumRunningNetworkMonotasks = new HashMap[Long, Int]()

  // Start the event thread.
  start()

  /**
   * Initializes the LocalDagScheduler by registering a {@link ExecutorBackend} and a
   * {@link MemoryStore}. The {@link MemoryStore} is used to determine when there is enough
   * memory to launch monotasks. It is currently used only by the {@link ComputeScheduler} (all
   * other resource schedulers ignore memory usage in determining whether to launch tasks).
   */
  def initialize(executorBackend: ExecutorBackend, memoryStore: MemoryStore): Unit = {
    this.executorBackend = Some(executorBackend)
    computeScheduler.initialize(memoryStore)
  }

  /** Returns the number of disks on the worker. */
  def getNumDisks(): Int = {
    diskScheduler.diskIds.size
  }

  def getNumRunningComputeMonotasks(): Int = {
    computeScheduler.numRunningTasks.get()
  }

  /**
   * Returns the total number of macrotasks that have monotasks running on this executor. This
   * includes macrotasks that are running remotely, but have monotasks running on this executor
   * to fetch data.
   */
  def getNumRunningMacrotasks(): Int = {
    runningMacrotaskAttemptIds.size
  }

  /**
   * Returns the number of macrotasks that are running on this executor. This does not include
   * macrotasks that are primarily running on a remote executor, but have monotask(s) running on
   * this executor to fetch data.
   */
  def getNumLocalRunningMacrotasks(): Int = {
    runningMacrotaskAttemptIds.size - remoteMacrotaskAttemptIdToRemainingMonotasks.size
  }

  def getDiskNameToNumRunningAndQueuedDiskMonotasks(): HashMap[String, Int] = {
    diskScheduler.getDiskNameToNumRunningAndQueuedDiskMonotasks
  }

  /**
   * Returns the number of macrotasks that have at least one compute monotask that is ready to be
   * run (i.e., all of its dependencies have finished) or is already running.
   */
  def getNumMacrotasksInCompute(): Long = macrotaskIdToNumRunningComputeMonotasks.size

  /**
   * Returns the number of macrotasks that have at least one disk monotask that is ready to be run
   * (i.e., all of its dependencies have finished) or is already running.
   */
  def getNumMacrotasksInDisk(): Long = macrotaskIdToNumRunningDiskMonotasks.size

  /**
   * Returns the number of macrotasks that have at least one network monotask that is ready to be
   * run (i.e., all of its dependencies have finished) or is already running.
   */
  def getNumMacrotasksInNetwork(): Long = macrotaskIdToNumRunningNetworkMonotasks.size

  def getOutstandingNetworkBytes(): Long = networkScheduler.getOutstandingBytes

  /**
   * This method processes events submitted to the LocalDagScheduler from external classes. It is
   * not thread safe, and will be called from a single-threaded event loop.
   */
  override protected def onReceive(event: LocalDagSchedulerEvent): Unit = event match {
    case SubmitMonotask(monotask) =>
      submitMonotask(monotask)

    case SubmitMonotasks(monotasks) =>
      monotasks.foreach(submitMonotask(_))

    case TaskSuccess(completedMonotask, serializedTaskResult) =>
      handleTaskSuccess(completedMonotask, serializedTaskResult)

    case TaskFailure(failedMonotask, serializedFailureReason) =>
      handleTaskFailure(failedMonotask, serializedFailureReason)

    case AddToMacrotask(monotask) =>
      addToMacrotask(monotask)
  }

  /** Called when an exception is thrown in the event loop. */
  override def onError(e: Throwable): Unit = {
    logError("LocalDagScheduler event loop failed", e)
    SparkUncaughtExceptionHandler.uncaughtException(e)
  }

 private def submitMonotask(monotask: Monotask): Unit = {
    if (monotask.dependenciesSatisfied()) {
      scheduleMonotask(monotask)
    } else {
      waitingMonotasks += monotask
    }
    val taskAttemptId = monotask.context.taskAttemptId
    logDebug(s"Submitting monotask $monotask (id: ${monotask.taskId}) for macrotask $taskAttemptId")
    runningMacrotaskAttemptIds += taskAttemptId

    if (monotask.context.taskIsRunningRemotely) {
      remoteMacrotaskAttemptIdToRemainingMonotasks.put(
        taskAttemptId,
        remoteMacrotaskAttemptIdToRemainingMonotasks.getOrElse(taskAttemptId, 0) + 1)
    }
  }

  private def getMapToUpdateNumRunningMonotasks(monotask: Monotask): HashMap[Long, Int] = {
    monotask match {
      case networkMonotask: NetworkMonotask => macrotaskIdToNumRunningNetworkMonotasks
      case computeMonotask: ComputeMonotask => macrotaskIdToNumRunningComputeMonotasks
      case diskMonotask: DiskMonotask => macrotaskIdToNumRunningDiskMonotasks
    }
  }

  private[monotasks] def updateMetricsForStartedMonotask(startedMonotask: Monotask): Unit = {
    val mapToUpdate = getMapToUpdateNumRunningMonotasks(startedMonotask)
    val macrotaskId = startedMonotask.context.taskAttemptId
    val currentlyRunning = mapToUpdate.getOrElse(macrotaskId, 0)
    mapToUpdate(macrotaskId) = currentlyRunning + 1
    startedMonotask.setQueueStartTime()
  }

  private[monotasks] def updateMetricsForFinishedMonotask(completedMonotask: Monotask) {
    val mapToUpdate = getMapToUpdateNumRunningMonotasks(completedMonotask)
    val macrotaskId = completedMonotask.context.taskAttemptId
    mapToUpdate.get(macrotaskId) match {
      case Some(numCurrentlyRunningMonotasks) =>
        if (numCurrentlyRunningMonotasks == 1) {
          mapToUpdate.remove(macrotaskId)
        } else {
          mapToUpdate(macrotaskId) = numCurrentlyRunningMonotasks - 1
        }

      case None =>
        logWarning(s"Not updating metrics for macrotask $macrotaskId because no record of it " +
          "could be found")
    }
  }

  private def sendStatusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    executorBackend.getOrElse {
      throw new IllegalStateException(
        s"Attempt to send update for task $taskId before an ExecutorBackend has been configured")
    }.statusUpdate(taskId, state, data)
  }

  /**
   * Marks the monotask as successfully completed by updating the dependency tree and running any
   * newly-runnable monotasks.
   *
   * @param completedMonotask The monotask that has completed.
   * @param serializedTaskResult If the monotask was the final monotask for the macrotask, a
   *                             serialized TaskResult to be sent to the driver (None otherwise).
   */
  private def handleTaskSuccess(
      completedMonotask: Monotask,
      serializedTaskResult: Option[ByteBuffer] = None): Unit = {
    val taskAttemptId = completedMonotask.context.taskAttemptId
    logDebug(s"Monotask $completedMonotask (id: ${completedMonotask.taskId}) for " +
      s"macrotask $taskAttemptId has completed.")
    completedMonotask.cleanup()
    updateMetricsForFinishedMonotask(completedMonotask)

    if (runningMacrotaskAttemptIds.contains(taskAttemptId)) {
      // If the macrotask has not failed, schedule any newly-ready monotasks.
      completedMonotask.dependents.foreach { monotask =>
        if (monotask.dependenciesSatisfied()) {
          if (waitingMonotasks.contains(monotask)) {
            scheduleMonotask(monotask)
          } else {
            logWarning(s"Monotask $monotask (id ${monotask.taskId}) is no longer in " +
              "waitingMonotasks, but it should not have been run yet, because one of its " +
              s"dependencies ($completedMonotask, id ${completedMonotask.taskId}) just finished.")
          }
        }
      }

      serializedTaskResult.map { result =>
        // Tell the executorBackend that the macrotask finished.
        runningMacrotaskAttemptIds.remove(taskAttemptId)
        completedMonotask.context.markTaskCompleted()
        logDebug(s"Notifying executorBackend about successful completion of task $taskAttemptId")
        sendStatusUpdate(taskAttemptId, TaskState.FINISHED, result)
      }

      if (completedMonotask.context.taskIsRunningRemotely) {
        remoteMacrotaskAttemptIdToRemainingMonotasks.get(taskAttemptId).foreach { numMonotasks =>
          val numRemainingMonotasks = numMonotasks - 1
          if (numRemainingMonotasks == 0) {
            runningMacrotaskAttemptIds.remove(taskAttemptId)
            remoteMacrotaskAttemptIdToRemainingMonotasks.remove(taskAttemptId)
          } else {
            remoteMacrotaskAttemptIdToRemainingMonotasks.put(taskAttemptId, numRemainingMonotasks)
          }
        }
      }
    } else {
      // This will only happen if another monotask in this macrotask failed while completedMonotask
      // was running, causing the macrotask to fail and its taskAttemptId to be removed from
      // runningMacrotaskAttemptIds. We should fail completedMonotask's dependents in case they have
      // not been failed already, which can happen if they are not dependents of the monotask that
      // failed.
      failDependentMonotasks(completedMonotask)
    }
    runningMonotasks.remove(completedMonotask.taskId)
  }

  /**
   * Marks the monotask and all monotasks that depend on it as failed and notifies the executor
   * backend that the associated macrotask has failed.
   *
   * @param failedMonotask The monotask that failed.
   * @param serializedFailureReason A serialized TaskFailedReason describing why the task failed.
   */
  private def handleTaskFailure(
      failedMonotask: Monotask, serializedFailureReason: Option[ByteBuffer]): Unit = {
    logInfo(s"Monotask ${failedMonotask.taskId} (for macrotask " +
      s"${failedMonotask.context.taskAttemptId}) failed")
    failedMonotask.cleanup()
    updateMetricsForFinishedMonotask(failedMonotask)

    runningMonotasks -= failedMonotask.taskId
    failDependentMonotasks(failedMonotask, Some(failedMonotask.taskId))
    val taskAttemptId = failedMonotask.context.taskAttemptId

    // Notify the executor backend that the macrotask has failed if we didn't already, and if the
    // monotask failure corresponded to a macrotask running on this machine.
    if (runningMacrotaskAttemptIds.remove(taskAttemptId)) {
      if (failedMonotask.context.taskIsRunningRemotely) {
        remoteMacrotaskAttemptIdToRemainingMonotasks.remove(taskAttemptId)
      } else {
        failedMonotask.context.markTaskCompleted()
        val failureReason = serializedFailureReason.getOrElse {
          throw new IllegalStateException(
            s"Expect a failure reason to be passed in when monotasks for local macrotasks fail")
        }
        sendStatusUpdate(taskAttemptId, TaskState.FAILED, failureReason)
      }
    }
  }

  private def failDependentMonotasks(
      monotask: Monotask,
      originalFailedTaskId: Option[Long] = None) {
    // TODO: We don't interrupt monotasks that are already running. See
    //       https://github.com/NetSys/spark-monotasks/issues/10
    val message = originalFailedTaskId.map { taskId =>
      s"it depended on monotask $taskId, which failed"
    }.getOrElse(s"another monotask in macrotask ${monotask.context.taskAttemptId} failed")

    monotask.dependents.foreach { dependentMonotask =>
      logDebug(s"Failing monotask ${dependentMonotask.taskId} because $message.")
      waitingMonotasks -= dependentMonotask
      failDependentMonotasks(dependentMonotask, originalFailedTaskId)
    }
  }

  /**
   * Submits a monotask to the relevant scheduler to be executed. This method should only be called
   * after all of the monotask's dependencies have been satisfied.
   */
  private def scheduleMonotask(monotask: Monotask) {
    assert(monotask.dependenciesSatisfied())
    updateMetricsForStartedMonotask(monotask)
    monotask match {
      case computeMonotask: ComputeMonotask => computeScheduler.submitTask(computeMonotask)
      case networkMonotask: NetworkMonotask => networkScheduler.submitTask(networkMonotask)
      case diskMonotask: DiskMonotask => diskScheduler.submitTask(diskMonotask)
      case _ => logError(s"Received unexpected type of monotask: $monotask")
    }
    /* Add the monotask to runningMonotasks before removing it from waitingMonotasks to avoid
     * a race condition in waitUntilAllTasksComplete where both sets are empty. */
    runningMonotasks += monotask.taskId
    waitingMonotasks.remove(monotask)
  }

  /**
   * Adds the provided monotask to the DAG of monotasks for the macrotask to which it belongs. In
   * order to make sure that the macrotask is not allowed to complete until all of its monotasks
   * have finished, the new monotask is added as a dependency of the macrotask's
   * ResultSerializationMonotask. This means that the macrotask's ResultSerializationMonotask must
   * have already been submitted but cannot have started executing yet. The best way to ensure this
   * is to only use this functionality from within the macrotask to which monotask belongs, and not
   * from within a PrepareMonotask or a ResultSerializationMonotask.
   */
  def addToMacrotask(monotask: Monotask): Unit = {
    val monotaskId = monotask.taskId
    val macrotaskId = monotask.context.taskAttemptId

    val monotasksFromSameMacrotask = waitingMonotasks.filter(_.context.taskAttemptId == macrotaskId)
    if (monotasksFromSameMacrotask.isEmpty) {
      throw new IllegalArgumentException(s"The macrotask (id: $macrotaskId) to which monotask " +
        s"$monotask (id: $monotaskId) belongs could not be found.")
    }

    val resultSerializationMonotask = monotasksFromSameMacrotask.find(
      _.isInstanceOf[ResultSerializationMonotask]).getOrElse(
          throw new IllegalArgumentException(
            s"The ResultSerializationMonotask for the macrotask (id: $macrotaskId) to which " +
            s"monotask $monotask (id: $monotaskId) belongs has either not been submitted or has " +
            s"already completed."))

    resultSerializationMonotask.addDependency(monotask)
    submitMonotask(monotask)
  }

  /**
   * For testing only. Waits until all macrotasks have completed, or until the specified time has
   * elapsed. Returns true if all macrotasks have completed and false if the specified amount of
   * time elapsed before all monotasks completed.
   *
   * Assumes no new macrotasks are submitted after this function is called (because otherwise
   * certain race conditions could lead to this returning true before the newly submitted macro-
   * tasks complete).
   */
  def waitUntilAllMacrotasksComplete(timeoutMillis: Int): Boolean = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!runningMacrotaskAttemptIds.isEmpty) {
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

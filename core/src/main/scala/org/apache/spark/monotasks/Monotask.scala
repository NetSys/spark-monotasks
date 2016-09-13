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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.HashSet

import org.apache.spark.{ExceptionFailure, Logging, SparkEnv, TaskContextImpl, TaskFailedReason}
import org.apache.spark.storage.{BlockId, MonotaskResultBlockId}

/**
 * A Monotask object encapsulates information about an operation that uses only one type of
 * resource. Subclasses contain task information specific to the resource that will be operated on,
 * and may include methods to actually interact with a resource.
 *
 * Monotasks are responsible for notifying the localDagScheduler when they have completed
 * successfully or when they have failed.
 */
private[spark] abstract class Monotask(val context: TaskContextImpl) extends Logging {
  val taskId = Monotask.newId()

  /** Whether this monotask has finished executing. */
  var isFinished = false

  /** The virtual size is used for deficit round robin queueing.
    *
    * This is used to ensure that the scheduler gives each phase of a multitask's execution equal
    * time, and should be set to (1 / # monotasks in phase).  For example, when a multitask writes
    * to disk with a single monotask, this should be set to 1.  If a multitask is reading from
    * disk using 10 monotasks, this should be set to 1/10.
    */
  var virtualSize: Double = 1

  /**
   * The BlockId with which this monotask stored temporary data in the BlockManager for use by its
   * dependencies, or None if the monotask did not store any temporary data.
   */
  protected var resultBlockId: Option[BlockId] = None

  /**
   * Monotasks that need to have finished before this monotask can be run. This includes all
   * dependencies (even ones that have already completed) so that we can clean up intermediate data
   * produced by dependencies when this monotask completes.
   */
  val dependencies = new HashSet[Monotask]()

  /** Monotasks that require this monotask to complete before they can be run. */
  val dependents = new HashSet[Monotask]()

  /**
   * If specified, this function will be called to handle an exception, and the LocalDagScheduler
   * will be told that the monotask was successful. This is used to circumvent the usual error
   * handling done in the LocalDagScheduler, which is useful for monotasks that are fetching data
   * for a remote executor, in which case we want to inform the remote executor about the failure
   * rather than handling it ourselves.
   */
  private var failureHandler: Option[TaskFailedReason => Unit] = None

  private var queueStartTimeNanos: Long = 0

  def setQueueStartTime(): Unit = queueStartTimeNanos = System.nanoTime

  /** Returns the elapsed time since the monotask began queuing. */
  def getQueueTime(): Long = System.nanoTime - queueStartTimeNanos

  private var startTimeNanos: Long = 0
  /**
   * Time it took this monotask to run. Initialize this to -1ms, so that it will appear as -1 (not
   * as 0) when returned from getRuntimeMillis().
   */
  private var runtimeNanos: Long = -1000000

  def setStartTime(): Unit = startTimeNanos = System.nanoTime

  def setFinishTime(): Unit = runtimeNanos = System.nanoTime - startTimeNanos

  /** Returns the time the monotask started, in millis. */
  def getStartTimeMillis(): Long = startTimeNanos / 1000000

  /** Returns the time the monotask ran for. */
  def getRuntimeMillis(): Long = runtimeNanos / 1000000

  override def toString(): String = {
    s"Monotask ${this.taskId} (${super.toString()} for macrotask ${context.taskAttemptId} in " +
      s"stage ${context.stageId})"
  }

  /**
   * Registers a monotask that must complete before this monotask can be run (by updating state
   * in both this monotask and in the passed dependency).
   */
  def addDependency(dependency: Monotask): Unit = {
    dependencies += dependency
    dependency.dependents += this
  }

  /**
   * Registers an exception handler to use instead of the usual exception handling that's done
   * in the LocalDagScheduler.
   *
   * @param alternateFailureHandler Function that accepts a TaskFailedReason and handles the given
   *                                failure.
   */
  def addAlternateFailureHandler(alternateFailureHandler: (TaskFailedReason => Unit)): Unit = {
    failureHandler = Some(alternateFailureHandler)
  }

  def getResultBlockId(): BlockId = {
    resultBlockId.getOrElse(throw new UnsupportedOperationException(
      s"Monotask $this (id: $taskId) does not have a result BlockId."))
  }

  /**
   * Returns true if all of the monotasks that this monotask depends on have completed (meaning
   * that this monotask can now be scheduled), or false otherwise.
   */
  def dependenciesSatisfied() =
    dependencies.filter(!_.isFinished).isEmpty

  /**
   * Marks this monotask as completed, and, if possible, cleans up its dependencies. A dependency
   * monotask can be cleaned up (its temporary data can be deleted and it can be removed from the
   * DAG of monotasks) if and only if all of its dependents have finished. This should be called
   * after this monotask either completes or fails.
   */
  def cleanup(): Unit = {
    isFinished = true
    dependencies.foreach { dependency =>
      if (dependency.dependents.count(!_.isFinished) == 0) {
        dependency.cleanupIntermediateData()
      }
    }
  }

  /**
   * Cleans up any intermediate data that was generated by this monotask. Should only be called once
   * all dependents have completed, because the dependents rely on this intermediate data.
   */
  protected def cleanupIntermediateData(): Unit = {
    logDebug(s"Monotask $this (id: $taskId) is being cleaned up...")
    resultBlockId.map { blockId =>
      val tellMaster = blockId match {
        case monotaskResultBlockId: MonotaskResultBlockId => false
        case _ => true
      }

      logDebug(s"Cleaning up result block id: $blockId")
      SparkEnv.get.blockManager.removeBlockFromMemory(blockId, tellMaster)
    }
  }

  /**
   * Utility method that serializes a TaskEndReason and tells the LocalDagScheduler that the
   * monotask failed.
   */
  def handleException(taskFailedReason: TaskFailedReason): Unit = {
    logError(s"Monotask $taskId failed with an exception: ${taskFailedReason.toErrorString}}")
    val env = SparkEnv.get
    val event = failureHandler match {
      case Some(handleTaskFailure) =>
        handleTaskFailure(taskFailedReason)
        // Tell the LocalDagScheduler that the task was successful, since the failure was handled
        // by failureHandler.
        TaskSuccess(this, None)

      case None =>
        // Set the task metrics, since this failure will cause the macrotask to be failed.
        context.taskMetrics.setMetricsOnTaskCompletion()
        val closureSerializer = env.closureSerializer.newInstance()
        TaskFailure(this, Some(closureSerializer.serialize(taskFailedReason)))
    }
    env.localDagScheduler.post(event)
  }

  def handleException(throwable: Throwable): Unit = {
    handleException(new ExceptionFailure(throwable, Some(context.taskMetrics)))
  }
}

private[spark] object Monotask {
  val nextId = new AtomicLong(0)

  def newId(): Long = nextId.getAndIncrement()
}

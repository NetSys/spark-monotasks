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

import org.apache.spark.{Accumulators, Logging, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.monotasks.{Monotask, TaskSuccess}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{SparkUncaughtExceptionHandler, Utils}

private[spark] abstract class ComputeMonotask(context: TaskContextImpl)
  extends Monotask(context) with Logging {

  /**
   * Runs the bulk of the computation for this monotask. If this monotask is the last one for the
   * macrotask, should return a result to be sent back to the driver.
   */
  protected def execute(): Option[ByteBuffer]

  private var accountingDone = false
  private var startTimeNanos = 0L

  /** Runs the execute method and handles common exceptions thrown by ComputeMonotasks. */
  def executeAndHandleExceptions() {
    // Set the class loader for the thread, which will be used by any broadcast variables that
    // are deserialized as part of the compute monotask.
    // TODO: Consider instead changing the thread factory to just
    //       automatically set the class loader each time a new thread is created (this is fine
    //       because the dependency manager is the same for all tasks in an executor).
    Thread.currentThread.setContextClassLoader(SparkEnv.get.dependencyManager.replClassLoader)

    startTimeNanos = System.nanoTime()
    try {
      Accumulators.registeredAccumulables.set(context.accumulators)
      TaskContext.setTaskContext(context)
      val result = execute()
      TaskContext.unset()
      this.setFinishTime()
      SparkEnv.get.localDagScheduler.post(TaskSuccess(this, result))
    } catch {
      case ffe: FetchFailedException => {
        // A FetchFailedException can be thrown by compute monotasks when local shuffle data
        // is missing from the block manager.
        handleException(ffe.toTaskFailedReason)
      }

      case cDE: CommitDeniedException => {
        handleException(cDE.toTaskFailedReason)
      }

      case t: Throwable => {
        // Attempt to exit cleanly by informing the driver of our failure.
        // If anything goes wrong (or this was a fatal exception), we will delegate to
        // the default uncaught exception handler, which will terminate the Executor.
        logError(s"Exception in TID ${context.taskAttemptId}", t)

        // Don't forcibly exit unless the exception was inherently fatal, to avoid
        // stopping other tasks unnecessarily.
        if (Utils.isFatalError(t)) {
          SparkUncaughtExceptionHandler.uncaughtException(t)
        }

        context.taskMetrics.setMetricsOnTaskCompletion()
        handleException(t)
      }
    } finally {
      accountForComputeTime()
    }
  }

  /**
   * Adds the time taken by this monotask to the macrotask's TaskMetrics, if it hasn't already been
   * done for this monotask. This method needs to check whether it was already called because
   * ResultSerializationMonotasks need to call this method themselves, before serializing the task
   * result (otherwise the update to the metrics won't be reflected in the serialized TaskMetrics
   * that are sent back to the driver).
   */
  protected def accountForComputeTime() {
    if (!accountingDone) {
      context.taskMetrics.incComputationNanos(System.nanoTime - startTimeNanos)
      accountingDone = true
    }
  }
}

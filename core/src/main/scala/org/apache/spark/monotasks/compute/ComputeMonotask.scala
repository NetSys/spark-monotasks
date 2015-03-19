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

import org.apache.spark.{Accumulators, ExceptionFailure, Logging, TaskContext}
import org.apache.spark.executor.ExecutorUncaughtExceptionHandler
import org.apache.spark.monotasks.Monotask
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils

private[spark] abstract class ComputeMonotask(context: TaskContext)
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
    startTimeNanos = System.nanoTime()
    try {
      Accumulators.registeredAccumulables.set(context.accumulators)
      val result = execute()
      context.localDagScheduler.handleTaskCompletion(this, result)
    } catch {
      case ffe: FetchFailedException => {
        // A FetchFailedException can be thrown by compute monotasks when local shuffle data
        // is missing from the block manager.
        val closureSerializer = context.env.closureSerializer.newInstance()
        context.localDagScheduler.handleTaskFailure(
          this, closureSerializer.serialize(ffe.toTaskEndReason))
      }

      case t: Throwable => {
        // Attempt to exit cleanly by informing the driver of our failure.
        // If anything goes wrong (or this was a fatal exception), we will delegate to
        // the default uncaught exception handler, which will terminate the Executor.
        logError(s"Exception in TID ${context.taskAttemptId}", t)

        // Don't forcibly exit unless the exception was inherently fatal, to avoid
        // stopping other tasks unnecessarily.
        if (Utils.isFatalError(t)) {
          ExecutorUncaughtExceptionHandler.uncaughtException(t)
        }

        context.taskMetrics.setMetricsOnTaskCompletion()
        val reason = ExceptionFailure(
          t.getClass.getName, t.getMessage, t.getStackTrace, Some(context.taskMetrics))
        val closureSerializer = context.env.closureSerializer.newInstance()
        context.localDagScheduler.handleTaskFailure(this, closureSerializer.serialize(reason))
      }
    } finally {
      accountForComputeTime()
    }
  }

  /**
   * Adds the time taken by this monotask to the macrotask's TaskMetrics, if it hasn't already been
   * done for this monotask. This method needs to check whether it was already called because
   * ExecutionMonotasks need to call this method themselves, before serializing the task result
   * (otherwise the update to the metrics won't be reflected in the serialized TaskMetrics
   * that are sent back to the driver).
   */
  protected def accountForComputeTime() {
    if (!accountingDone) {
      context.taskMetrics.computationNanos += System.nanoTime - startTimeNanos
      accountingDone = true
    }
  }
}

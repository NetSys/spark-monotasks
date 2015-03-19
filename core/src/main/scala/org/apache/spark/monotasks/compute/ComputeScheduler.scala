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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.Logging
import org.apache.spark.util.Utils

private[spark] class ComputeScheduler() extends Logging {
  private val threads = Runtime.getRuntime.availableProcessors()

  // TODO: This threadpool currently uses a single FIFO queue when the number of tasks exceeds the
  //       number of threads; eventually, we'll want a smarter queueing strategy.
  private val computeThreadpool = Utils.newDaemonFixedThreadPool(threads, "compute-monotask-thread")

  val numRunningTasks = new AtomicInteger(0)

  logDebug(s"Started ComputeScheduler with $threads parallel threads")

  def submitTask(monotask: ComputeMonotask) {
    computeThreadpool.execute(new Runnable {
      override def run(): Unit = {
        numRunningTasks.incrementAndGet()
        // Set the class loader for the thread, which will be used by any broadcast variables that
        // are deserialized as part of the compute monotask.
        // TODO: Consider instead changing the thread factory to just
        //       automatically set the class loader each time a new thread is created (this is fine
        //       because the dependency manager is the same for all tasks in an executor).
        Thread.currentThread.setContextClassLoader(
          monotask.context.dependencyManager.replClassLoader)
        monotask.executeAndHandleExceptions()
        numRunningTasks.decrementAndGet()
      }
    })
  }
}

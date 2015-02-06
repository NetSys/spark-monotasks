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

package org.apache.spark.monotasks.network

import org.apache.spark.Logging
import org.apache.spark.util.Utils

private[spark] class NetworkScheduler() extends Logging {
  // TODO: Change this to instead launch a new network task iff there is spare network capacity.
  // TODO: this doesn't really do anything now, because the network call is launched asynchronously
  //       in NetworkMonotask.
  private val threads = Runtime.getRuntime.availableProcessors()
  logInfo(s"Started NetworkScheduler with $threads parallel threads")

  // TODO: This threadpool currently uses a single FIFO queue when the number of tasks exceeds the
  //       number of threads; eventually, we'll want a smarter queueing strategy.
  private val networkThreadpool = Utils.newDaemonFixedThreadPool(threads, "network-monotask-thread")

  def submitTask(monotask: NetworkMonotask) {
    networkThreadpool.execute(new Runnable {
      override def run(): Unit = monotask.execute()
    })
  }
}

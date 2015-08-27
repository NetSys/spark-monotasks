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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.Logging
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.storage.MemoryStore

private[spark] sealed trait RunningTasksUpdate
private[spark] object TaskStarted extends RunningTasksUpdate
private[spark] object TaskCompleted extends RunningTasksUpdate

private[spark] class ComputeScheduler(
    private val threads: Int = Runtime.getRuntime.availableProcessors()) extends Logging {
  private var executorBackend: Option[ExecutorBackend] = None
  private var memoryStore: Option[MemoryStore] = None

  /**
   * Queue of all monotasks that are waiting to be run. We currently just use a FIFO queue; we
   * may want to implement a more sophisticated queueing mechanism in the future.
   */
  private val monotaskQueue = new LinkedBlockingQueue[ComputeMonotask]

  /**
   * Queue of monotasks that have been quarantined because they require memory to run, and when
   * they reached the beginning of {@link monotaskQueue}, no memory was available.
   */
  private val quarantineQueue = new LinkedBlockingQueue[ComputeMonotask]

  val numRunningTasks = new AtomicInteger(0)

  /** This must be called before any tasks are submitted. */
  def setExecutorBackend(executorBackend: ExecutorBackend): Unit = {
    this.executorBackend = Some(executorBackend)
  }

  /** This must be called before any tasks are submitted. */
  def setMemoryStore(memoryStore: MemoryStore): Unit = {
    this.memoryStore = Some(memoryStore)

    memoryStore.registerBlockRemovalCallback(handleBlockRemovedFromMemoryStore)
    startComputeMonotaskThreads()
  }

  /** Starts threads to run {@link ComputeMonotask}s. */
  private def startComputeMonotaskThreads(): Unit = {
    (1 to threads).foreach { i =>
      val thread = new Thread(new ConsumerThread())
      thread.setDaemon(true)
      thread.setName(s"Compute Thread $i")
      thread.start()
    }

    logDebug(s"Started ComputeScheduler with $threads parallel threads")
  }

  private def updateRunningTasksAndNotifyBackend(updateType: RunningTasksUpdate) {
    val currentlyRunningTasks = updateType match {
      case TaskStarted =>
        numRunningTasks.incrementAndGet()
      case TaskCompleted =>
        numRunningTasks.decrementAndGet()
    }
    val freeCores =
      threads - currentlyRunningTasks - monotaskQueue.size() - quarantineQueue.size()
    // TODO: We may want to consider updating the driver less frequently, otherwise these messages
    //       to the driver may become a bottleneck.
    executorBackend.getOrElse {
      throw new IllegalStateException(
        "An ExecutorBackend must be set in ComputeScheduler before tasks are launched")
    }.updateFreeCores(freeCores)
  }

  /**
   * Submits a monotask to be scheduled as soon as sufficient CPU and memory resources are
   * available.
   */
  def submitTask(monotask: ComputeMonotask): Unit = synchronized {
    monotaskQueue.put(monotask)
    this.notify()
  }

  /**
   * Retrieves a Monotask to run, waiting if necessary until a monotask for which there is
   * sufficient memory to run becomes available.
   */
  private def takeMonotask(): ComputeMonotask = synchronized {
    var monotaskToRun: Option[ComputeMonotask] = pollForMonotask()
    while (monotaskToRun.isEmpty) {
      this.wait()
      monotaskToRun = pollForMonotask()
    }
    monotaskToRun.get
  }

  private def pollForMonotask(): Option[ComputeMonotask] = synchronized {
    val freeMemory = memoryStore.getOrElse {
      throw new IllegalStateException(
        "A MemoryStore must be set in ComputeScheduler before tasks are launched")
    }.freeMemory

    if (freeMemory > 0) {
      // Any kind of monotask can be run, so first look to see if there are any monotasks that
      // are waiting until there's available memory to run.
      val monotaskFromQuarantine = quarantineQueue.poll()
      if (monotaskFromQuarantine != null) {
        Some(monotaskFromQuarantine)
      } else {
        Option(monotaskQueue.poll())
      }
    } else {
      // No free memory is available, so only run monotasks that won't generate new in-memory
      // data. For now, we assume that ShuffleMapMonotasks will generate new in-memory data,
      // and that all other types of monotasks do not.
      var monotaskToRun: Option[ComputeMonotask] = None
      while (monotaskToRun.isEmpty && !monotaskQueue.isEmpty) {
        monotaskQueue.poll() match {
          case shuffleMapMonotask: ShuffleMapMonotask[_] =>
            // Because ShuffleMapMonotasks generate shuffle data that needs to be temporarily stored
            // in-memory, wait until there is some free memory to launch the task.
            quarantineQueue.put(shuffleMapMonotask)
          case monotask: Any =>
            monotaskToRun = Some(monotask)
        }
      }
      monotaskToRun
    }
  }

  private def handleBlockRemovedFromMemoryStore(freeMemory: Long): Unit = synchronized {
    if (freeMemory > 0) {
      // TODO: Consider tracking whether there was already free memory, and only calling
      //       notify() when the amount of free memory is newly greater than 0.
      this.notify()
    }
  }

  private class ConsumerThread extends Runnable {
    def run(): Unit = {
      while (true) {
        val monotask = takeMonotask()
        updateRunningTasksAndNotifyBackend(TaskStarted)
        monotask.executeAndHandleExceptions()
        updateRunningTasksAndNotifyBackend(TaskCompleted)
      }
    }
  }
}

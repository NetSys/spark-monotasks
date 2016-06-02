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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.{Logging, SparkConf, SparkException}

private[spark] class NetworkScheduler(conf: SparkConf) extends Logging {
  /** Number of bytes that this executor is currently waiting to receive over the network. */
  private var currentOutstandingBytes = new AtomicLong(0)

  /**
   * Queue of monotasks waiting to be executed. submitMonotask() puts monotasks in this queue,
   * and a separate thread executes them, so that launching network monotasks doesn't happen
   * in the main scheduler thread (network monotasks are asynchronous, but launching a large
   * number of them can still take a non-negligible amount of time in aggregate).
   */
  private val readyMonotaskQueue = new LinkedBlockingQueue[NetworkMonotask]()

  /**
   * Queue of low priority monotasks that should only be executed when the network scheduler has
   * no other work to do.
   */
  private val lowPriorityNetworkRequestMonotaskQueue =
    new LinkedBlockingQueue[NetworkRequestMonotask]()

  /**
   * Queue of NetworkRequestMonotasks that can't be sent until a task finishes. This is guaranteed
   * to be grouped by task ID (a SubmitMonotasks event will be submitted to the LocalDagScheduler
   * with all monotasks for a given task, so they'll all be submitted to the per-resource
   * schedulers in order, and no others will get inserted in between).
   */
  private val networkRequestMonotaskQueue = new LinkedBlockingQueue[NetworkRequestMonotask]()

  /** For each task with running network requests, the number of outstanding requests. */
  private val taskIdToNumOutstandingRequests = new HashMap[Long, Int]()

  /** Maximum number of tasks that can concurrently have outstanding network requests. */
  private val maxConcurrentTasks = conf.getInt("spark.monotasks.network.maxConcurrentTasks", 4)

  /** Ids of NetworkResponseMonotasks that are currently transmitting data over the network. */
  private val runningResponseMonotasks = new HashSet[Long]()
  /** End time of the last NetworkResponseMonotask to finish. */
  private var lastTransmitEndNanos = 0L
  /** The total time during which there were no blocks being sent by the current worker. */
  private var transmitTotalIdleNanos = 0L

  initializeIdleTimeMeasurement()

  // Start a thread responsible for executing the network monotasks in monotaskQueue.
  private val monotaskLaunchThread = new Thread(new Runnable() {
    override def run(): Unit = {
      while (true) {
        readyMonotaskQueue.take().execute(NetworkScheduler.this)
      }
    }
  })
  monotaskLaunchThread.setDaemon(true)
  monotaskLaunchThread.setName("Network monotask launch thread")
  monotaskLaunchThread.start()

  def submitTask(monotask: NetworkMonotask): Unit = {
    monotask match {
      case networkResponseMonotask: NetworkResponseMonotask =>
        // This logging with the special "KNET" keyword exists so it's easy to grep for it
        // in the executor logs and generate corresponding graphs about network performance.
        logInfo(s"KNET Monotask ${monotask.taskId} block ${networkResponseMonotask.blockId} to " +
          s"${networkResponseMonotask.channel.remoteAddress()} READY ${System.currentTimeMillis}")
        readyMonotaskQueue.put(monotask)

      case networkRequestMonotask: NetworkRequestMonotask =>
        if (maxConcurrentTasks <= 0) {
          logInfo(s"Max concurrent tasks is $maxConcurrentTasks, so launching monotask immediately")
          readyMonotaskQueue.put(monotask)
        } else {
          val taskId = networkRequestMonotask.context.taskAttemptId
          taskIdToNumOutstandingRequests.synchronized {
            if (taskIdToNumOutstandingRequests.contains(taskId) ||
              taskIdToNumOutstandingRequests.size < maxConcurrentTasks) {
              logInfo(s"Max concurrent tasks $maxConcurrentTasks; launching " +
                s"NetworkRequestMonotask for $taskId")
              // Start the monotask and update the counts.
              readyMonotaskQueue.put(monotask)
              taskIdToNumOutstandingRequests.put(
                taskId,
                taskIdToNumOutstandingRequests.getOrElse(taskId, 0) + 1)
            } else {
              logInfo(s"$maxConcurrentTasks tasks are already running, so " +
                s"queueing $networkRequestMonotask (" +
                s"${if (networkRequestMonotask.isLowPriority()) "low" else "high"} priority) for " +
                s"macrotask $taskId")
              if (networkRequestMonotask.isLowPriority()) {
                lowPriorityNetworkRequestMonotaskQueue.put(networkRequestMonotask)
              } else {
                networkRequestMonotaskQueue.put(networkRequestMonotask)
              }
            }
          }
        }

      case _ =>
        logError("Unknown type of monotask! " + monotask)
        throw new SparkException("Unknown type of monotask in network scheduler! " + monotask)
    }
  }

    /**
     * Updates metadata about which tasks are currently using the network, and possibly launches
     * more network requests.
     */
  def handleNetworkRequestSatisfied(monotask: NetworkRequestMonotask): Unit = {
    if (maxConcurrentTasks <= 0) {
      // We're not doing any throttling of how many network monotasks run concurrently, so just
      // return.
      return
    }
    val taskId = monotask.context.taskAttemptId
    taskIdToNumOutstandingRequests.synchronized {
      val numOutstandingRequestsForTask = taskIdToNumOutstandingRequests(taskId)
      if (numOutstandingRequestsForTask > 1) {
        // Decrement it and don't launch any more network requests for different macrotasks.
        taskIdToNumOutstandingRequests.put(taskId, numOutstandingRequestsForTask - 1)
      } else {
        // Remove the macrotask from the outstanding requests and possibly start a new macrotask's
        // network requests.
        taskIdToNumOutstandingRequests.remove(taskId)
        if (networkRequestMonotaskQueue.size() + lowPriorityNetworkRequestMonotaskQueue.size() >
            0) {
          var tasksStarted = 0
          // Try to launch all of the network monotasks for a particular macrotask.
          // TODO: For the low-priority ones, all of a macrotask's monotasks will be fetching data
          //       from the same machine. This may cause network hotspots, in which case we should
          //       organize these differently.
          //       One better strategy would be to have one queue per remote execute, and do
          //       round-robin on the executors up to some maximum amount of outstanding data.
          val monotaskQueue = if (networkRequestMonotaskQueue.size() > 0) {
            logInfo(s"Attempting to launch high priority network monotask")
            networkRequestMonotaskQueue
          } else {
            logInfo(s"Attempting to launch low priority network monotask")
            lowPriorityNetworkRequestMonotaskQueue
          }

          // Thread safety isn't an issue here, because access is synchronized on
          // taskIdToNumOutstandingRequests.
          val firstTaskId = monotaskQueue.peek().context.taskAttemptId

          while (monotaskQueue.size() > 0 &&
              monotaskQueue.peek().context.taskAttemptId == firstTaskId) {
            val networkMonotask = monotaskQueue.remove()
            // TODO: unify networkRequestMonotaskQueue with readyMonotaskQueue. If this method
            //       handles updating taskIdToNumOutstanding, the thread that launches monotasks
            //       can read them directly from networkRequestMonotaskQueue.
            logInfo(s"Launching network monotask $networkMonotask for task $firstTaskId")
            readyMonotaskQueue.put(networkMonotask)
            tasksStarted += 1
          }
          logInfo(s"Starting $tasksStarted monotasks")
          taskIdToNumOutstandingRequests.put(firstTaskId, tasksStarted)
        }
      }
    }
  }

  /**
   * Used to keep track of the bytes outstanding over the network. Can be called with a negative
   * value to indicate bytes that are no longer outstanding.
   */
  def addOutstandingBytes(bytes: Long) = currentOutstandingBytes.addAndGet(bytes)

  def getOutstandingBytes: Long = currentOutstandingBytes.get()

  /**
   * Called when a NetworkResponseMonotask is about to send its data. Updates internal metadata
   * related to the total time that this NetworkScheduler has not been transmitting.
   */
  def updateIdleTimeOnResponseStart(
      response: NetworkResponseMonotask,
      currentTimeNanos: Long = System.nanoTime()): Unit = synchronized {
    val idleNanos = currentTimeNanos - lastTransmitEndNanos
    // Only update transmitTotalIdleNanos if idleNanos is positive to avoid a race condition. If
    // there are other NetworkResponseMonotasks currently running, then we do not need to update the
    // idle time because one of the other monotasks will have already done so.
    if ((idleNanos > 0) && (runningResponseMonotasks.size == 0)) {
      transmitTotalIdleNanos += idleNanos
    }
    runningResponseMonotasks += response.taskId
  }

  /**
   * Called when a NetworkResponseMonotask has finished sending its data, or failed. Updates
   * internal metadata related to the total time that the NetworkScheduler has not been
   * transmitting.
   */
  def updateIdleTimeOnResponseEnd(
      response: NetworkResponseMonotask,
      currentTimeNanos: Long = System.nanoTime()): Unit = synchronized {
    if (currentTimeNanos > lastTransmitEndNanos) {
      lastTransmitEndNanos = currentTimeNanos
    }
    runningResponseMonotasks -= response.taskId
  }

  /**
   * Configure the time that the transmission idle time measurements should treat as the time that
   * this NetworkScheduler was created. Must be called before this NetworkScheduler is used.
   */
  def initializeIdleTimeMeasurement(startNanos: Long = System.nanoTime()): Unit = synchronized {
    lastTransmitEndNanos = startNanos
  }

  def getTransmitTotalIdleMillis(
      currentTimeNanos: Long = System.nanoTime()): Double = synchronized {
    // If there are no NetworkResponseMonotasks currently running, then we need to account for the
    // idle time since the last one finished.
    val undocumentedIdleNanos = if (runningResponseMonotasks.size == 0) {
      currentTimeNanos - lastTransmitEndNanos
    } else {
      0
    }
    (transmitTotalIdleNanos + undocumentedIdleNanos).toDouble / 1000000
  }
}

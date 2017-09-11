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

import java.util.concurrent.{LinkedBlockingQueue, PriorityBlockingQueue}
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
   * no other work to do. This queue is ordered by the reduce IDs that the monotasks correspond
   * to.
   * TODO: Figure out what to do here when there are multiple jobs running concurrently (in which
   *       cases there will be tasks with the same reduce ID, but corresponding to different jobs).
   */
  private val lowPriorityNetworkRequestMonotaskQueue =
    new PriorityBlockingQueue[NetworkRequestMonotask]()

  /**
   * Queue of NetworkRequestMonotasks that can't be sent until a task finishes. This is guaranteed
   * to be grouped by task ID (a SubmitMonotasks event will be submitted to the LocalDagScheduler
   * with all monotasks for a given task, so they'll all be submitted to the per-resource
   * schedulers in order, and no others will get inserted in between).
   */
  private val networkRequestMonotaskQueue = new LinkedBlockingQueue[NetworkRequestMonotask]()

  /**
   * For each task with running network requests, the number of outstanding requests. This
   * mapping is based on the reduce ID (which is different than the task ID -- the reduce ID
   * starts at 0 for each new stage) so that it can be used for the opportunistic early monotasks,
   * for which the task ID isn't yet known.
   */
  private val reduceIdToNumOutstandingRequests = new HashMap[Long, Int]()

  /** Maximum number of tasks that can concurrently have outstanding network requests. */
  private val maxConcurrentTasks = NetworkScheduler.getMaxConcurrentTasks(conf)

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
        val monotask = readyMonotaskQueue.take()
        monotask.setStartTime()
        monotask.execute(NetworkScheduler.this)
      }
    }
  })
  monotaskLaunchThread.setDaemon(true)
  monotaskLaunchThread.setName("Network monotask launch thread")
  monotaskLaunchThread.start()

  /**
   * Launches the new request monotask, if the maximum number of outstanding requests hasn't been
   * reached, or otherwise queues the monotask.
   */
  private def launchOrQueueMonotask(monotask: NetworkRequestMonotask): Unit = {
    if (maxConcurrentTasks <= 0) {
      logInfo(s"Launching monotask immediately, because there's no throttling on the number of " +
        "concurrent tasks")
      readyMonotaskQueue.put(monotask)
    } else {
      reduceIdToNumOutstandingRequests.synchronized {
        val canLaunch = reduceIdToNumOutstandingRequests.size < maxConcurrentTasks || {
          val earlierTasksWaiting = (monotask.isLowPriority() &&
            !lowPriorityNetworkRequestMonotaskQueue.isEmpty &&
            lowPriorityNetworkRequestMonotaskQueue.peek().reduceId <= monotask.reduceId)
          reduceIdToNumOutstandingRequests.contains(monotask.reduceId) && !earlierTasksWaiting
        }

        val taskId = monotask.context.taskAttemptId
        if (canLaunch) {
          logInfo(s"Max concurrent tasks $maxConcurrentTasks; launching NetworkRequestMonotask " +
            s"for $taskId, reduce ${monotask.reduceId}")
          readyMonotaskQueue.put(monotask)
          reduceIdToNumOutstandingRequests.put(
            monotask.reduceId,
            reduceIdToNumOutstandingRequests.getOrElse(monotask.reduceId, 0) + 1)
        } else {
          logInfo(s"$maxConcurrentTasks are already running (or monotasks for lower task IDs are " +
            s"waiting in the queue), so queueing $monotask (" +
            s"${if (monotask.isLowPriority()) "low" else "high"} priority)")
          if (monotask.isLowPriority()) {
            lowPriorityNetworkRequestMonotaskQueue.put(monotask)
          } else {
            networkRequestMonotaskQueue.put(monotask)
          }
        }
      }
    }
  }

  def submitTask(monotask: NetworkMonotask): Unit = {
    monotask match {
      case networkResponseMonotask: NetworkResponseMonotask =>
        // This logging with the special "KNET" keyword exists so it's easy to grep for it
        // in the executor logs and generate corresponding graphs about network performance.
        logInfo(s"KNET Monotask ${monotask.taskId} block ${networkResponseMonotask.blockId} to " +
          s"${networkResponseMonotask.channel.remoteAddress()} READY ${System.currentTimeMillis}")
        readyMonotaskQueue.put(monotask)

      case networkRequestMonotask: NetworkRequestMonotask =>
        launchOrQueueMonotask(networkRequestMonotask)

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
    val finishedReduceId = monotask.reduceId
    reduceIdToNumOutstandingRequests.synchronized {
      val numOutstandingRequestsForTask = reduceIdToNumOutstandingRequests(finishedReduceId)
      if (numOutstandingRequestsForTask > 1) {
        // Decrement it and don't launch any more network requests for different reduce tasks.
        reduceIdToNumOutstandingRequests.put(finishedReduceId, numOutstandingRequestsForTask - 1)
      } else {
        // Remove the reduce ID from the outstanding requests and possibly start the network
        // requests for a new task.
        // NB: This may go back to reducers with lower IDs, as map tasks finish.
        // TODO: With the early reducers, might make more sense to do this by amount of data rather
        //       than by number of outstanding requests.
        reduceIdToNumOutstandingRequests.remove(finishedReduceId)
        if (networkRequestMonotaskQueue.size() + lowPriorityNetworkRequestMonotaskQueue.size() >
            0) {
          var tasksStarted = 0
          // Try to launch all of the network monotasks for a particular reduce task.
          val monotaskQueue = if (networkRequestMonotaskQueue.size() > 0) {
            logInfo(s"Attempting to launch high priority network monotask")
            networkRequestMonotaskQueue
          } else {
            logInfo(s"Attempting to launch low priority network monotask")
            lowPriorityNetworkRequestMonotaskQueue
          }

          // Thread safety isn't an issue here, because access is synchronized on
          // taskIdToNumOutstandingRequests.
          val firstReduceId = monotaskQueue.peek().reduceId

          while (monotaskQueue.size() > 0 &&
              monotaskQueue.peek().reduceId == firstReduceId) {
            val networkMonotask = monotaskQueue.remove()
            // TODO: unify networkRequestMonotaskQueue with readyMonotaskQueue. If this method
            //       handles updating taskIdToNumOutstanding, the thread that launches monotasks
            //       can read them directly from networkRequestMonotaskQueue.
            logInfo(s"Launching network monotask $networkMonotask for reduce $firstReduceId")
            readyMonotaskQueue.put(networkMonotask)
            tasksStarted += 1
          }
          reduceIdToNumOutstandingRequests.put(
            firstReduceId,
            reduceIdToNumOutstandingRequests.getOrElse(firstReduceId, 0) + tasksStarted)
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

object NetworkScheduler {
  val DEFAULT_CONCURRENT_NETWORK_MONOTASKS = 4

  def getMaxConcurrentTasks(conf: SparkConf): Int = {
    conf.getInt("spark.monotasks.network.maxConcurrentTasks",
      NetworkScheduler.DEFAULT_CONCURRENT_NETWORK_MONOTASKS)
  }
}

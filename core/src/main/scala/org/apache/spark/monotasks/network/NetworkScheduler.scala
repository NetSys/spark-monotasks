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

import scala.collection.mutable.HashSet

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

  /** Maximum number of bytes that can be outstanding over the network at once. */
  private val maxOutstandingBytes =
    conf.getInt("spark.monotasks.network.maxOutstandingBytes", 3 * 1024 * 1024)

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

  private def maybeLaunchMonotasks(): Unit = synchronized {
    while (currentOutstandingBytes.get() < maxOutstandingBytes &&
      !(lowPriorityNetworkRequestMonotaskQueue.isEmpty && networkRequestMonotaskQueue.isEmpty)) {
      val monotask = if (!networkRequestMonotaskQueue.isEmpty) {
        networkRequestMonotaskQueue.take()
      } else {
        lowPriorityNetworkRequestMonotaskQueue.take()
      }
      logDebug(s"Launching NetworkRequestMonotask $monotask when current outstanding bytes " +
        s"is ${currentOutstandingBytes.get()} and max outstanding bytes is $maxOutstandingBytes.")
      addOutstandingBytes(monotask.totalBytes)
      readyMonotaskQueue.put(monotask)
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
        if (networkRequestMonotask.isLowPriority()) {
          lowPriorityNetworkRequestMonotaskQueue.put(networkRequestMonotask)
        } else {
          networkRequestMonotaskQueue.put(networkRequestMonotask)
        }
        maybeLaunchMonotasks()

      case _ =>
        logError("Unknown type of monotask! " + monotask)
        throw new SparkException("Unknown type of monotask in network scheduler! " + monotask)
    }
  }

  /**
   * Used to keep track of the bytes outstanding over the network. Can be called with a negative
   * value to indicate bytes that are no longer outstanding.
   */
  def addOutstandingBytes(bytes: Long) = {
    currentOutstandingBytes.addAndGet(bytes)
    maybeLaunchMonotasks()
  }

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

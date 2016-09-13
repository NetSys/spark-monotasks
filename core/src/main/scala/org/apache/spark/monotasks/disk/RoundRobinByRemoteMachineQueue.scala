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

package org.apache.spark.monotasks.disk

import scala.collection.mutable.{ArrayBuffer, HashMap, Queue}

import org.apache.spark.Logging

/**
 * A queue that round-robins over all machines (including the local machine).
 */
private[spark] class RoundRobinByRemoteMachineQueue() extends Logging {
  // For each machine, a FIFO queue of those monotasks.
  private val remoteMachineToQueue = new HashMap[String, Queue[DiskMonotask]]()
  // There are a fixed number of remote machines (for now), so we assume we'll never need to
  // remove anything from this list.
  private val remoteMachines = new ArrayBuffer[String]
  private var currentIndex = 0

  def enqueue(monotask: DiskMonotask): Unit = synchronized {
    val remoteName = monotask.context.remoteName
    val queue = remoteMachineToQueue.get(remoteName).getOrElse {
      val newQueue = new Queue[DiskMonotask]()
      remoteMachineToQueue.put(remoteName, newQueue)
      remoteMachines.append(remoteName)
      newQueue
    }
    queue.enqueue(monotask)
    notify()
  }

  def dequeue(): DiskMonotask = synchronized {
    while (true) {
      (0 until remoteMachines.length).foreach {i =>
        val currentRemoteMachine = remoteMachines(currentIndex)
        // Update currentIndex
        currentIndex = (currentIndex + 1) % remoteMachines.length
        val queue = remoteMachineToQueue(currentRemoteMachine)
        if (!queue.isEmpty) {
          logInfo(s"Running task from ${currentRemoteMachine}. Other queues are " +
            s"${remoteMachineToQueue.toSeq.map(pair => (pair._1, pair._2.length))}")
          return queue.dequeue()
        }
      }
      wait()
    }
    // This exception is needed to satisfy the Scala compiler.
    throw new Exception("Should not reach this state")
  }

  def headOption(): Option[DiskMonotask] = synchronized {
    if (isEmpty()) {
      None
    } else {
      // Don't want to actually update current index, because may not dequeue anything
      // right now (and the correct thing to dequeue may change).
      var tempCurrentIndex = currentIndex
      (0 until remoteMachines.length).foreach {i =>
        val currentRemoteMachine = remoteMachines(tempCurrentIndex)
        // Update currentIndex
        tempCurrentIndex = (tempCurrentIndex + 1) % remoteMachines.length
        val queue = remoteMachineToQueue(currentRemoteMachine)
        if (!queue.isEmpty) {
          return Some(queue.head)
        }
      }
      throw new Exception("Should not reach this state; check isEmpty() correctness")
    }
  }

  def isEmpty(): Boolean = {
    remoteMachineToQueue.forall {
      case (_, queue) => queue.isEmpty
    }
  }
}

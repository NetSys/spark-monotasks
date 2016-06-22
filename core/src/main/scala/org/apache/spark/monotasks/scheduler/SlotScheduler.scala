/*
 * Copyright 2016 The Regents of The University California
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
package org.apache.spark.monotasks.scheduler

import scala.collection.mutable.Set

import org.apache.spark.SparkConf
import org.apache.spark.monotasks.MonotaskType
import org.apache.spark.scheduler.{TaskSet, WorkerOffer}
import org.apache.spark.Logging

/** A MonotasksScheduler that assigns a fixed number of monotasks to each worker machine. */
private[spark] class SlotScheduler(conf: SparkConf) extends MonotasksScheduler with Logging {
  /**
   * Number of tasks that may be running concurrently on the network on an executor.
   *
   * Always at least 1, because when this is configured to 0, it means there's no limit on
   * the number of tasks that can be using the network concurrently.
   */
  private val concurrentNetworkTasks = Math.max(
    1, conf.getInt("spark.monotasks.network.maxConcurrentTasks", 4))

  /** Called on the driver to determine how many tasks to launch initially. */
  def getInitialOffers(
      originalWorkerOffers: Seq[WorkerOffer], taskSet: TaskSet): Seq[WorkerOffer] = {
    val networkSlotsToAdd = if (taskSet.usesNetwork) concurrentNetworkTasks else 0
    originalWorkerOffers.map {  originalOffer =>
      val diskSlotsToAdd = if (taskSet.usesDisk) originalOffer.totalDisks else 0
      val newSlots = originalOffer.freeSlots + networkSlotsToAdd + diskSlotsToAdd
      logInfo(s"Original offer for ${originalOffer.host} with ${originalOffer.freeSlots} " +
        s"has been increased to add $diskSlotsToAdd disk slots and $networkSlotsToAdd " +
        s"network slot ($newSlots total slots)")
      new WorkerOffer(
        originalOffer.executorId, originalOffer.host, newSlots, originalOffer.totalDisks)
    }
  }

  /** Called on a worker each time a task changes its phase of execution. */
  def handlePhaseChange(
      stageId: Int,
      previousPhase: Option[(MonotaskType.Value, Int)],
      newPhases: Set[(MonotaskType.Value, Int)],
      macrotaskIsFinished: Boolean): Unit = {
    logInfo(
      s"Task in stage $stageId ${previousPhase.map(p => s"finished phase ${p._2}").getOrElse("")}" +
      s" and finished is $macrotaskIsFinished")
    if (macrotaskIsFinished) {
      logInfo(s"Task for stage $stageId has completed, so requesting new task from driver")
      getExecutorBackend().requestTasks(1)
    }
  }
}

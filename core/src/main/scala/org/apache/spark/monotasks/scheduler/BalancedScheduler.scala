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

import org.apache.spark.Logging
import org.apache.spark.monotasks.MonotaskType
import org.apache.spark.scheduler.{TaskSet, WorkerOffer}

/** A MonotasksScheduler that evenly divides a job's tasks between machines. */
private[spark] class BalancedScheduler extends MonotasksScheduler with Logging {

  /**
   * Generates initial offers such that the tasks will be evenly distributed across the given
   * workers, and assigned immediately.
   */
  def getInitialOffers(
      originalWorkerOffers: Seq[WorkerOffer], taskSet: TaskSet): Seq[WorkerOffer] = {
    val numWorkers = originalWorkerOffers.size
    val numTasks = taskSet.tasks.length.toDouble
    val tasksPerWorker = Math.ceil(numTasks / numWorkers).toInt
    originalWorkerOffers.map {
      case WorkerOffer(executorId, host, freeSlots, numDisks) =>
        // To be safe, create a new WorkerOffer, but it would probably be fine to modify the old
        // one instead.
        logInfo(s"Changing worker offer for $host from $freeSlots free slots to $tasksPerWorker " +
          s"(to accommodate $numTasks total tasks)")
        new WorkerOffer(executorId, host, tasksPerWorker, numDisks)
    }
  }

  /** Called on a worker each time a task changes its phase of execution. */
  @Override def handlePhaseChange(
      stageId: Int,
      previousPhase: Option[(MonotaskType.Value, Int)],
      newPhaseType: Set[(MonotaskType.Value, Int)],
      macrotaskIsFinished: Boolean): Unit = {
    // Do nothing! All of the tasks should have been assigned at the beginning.
  }
}

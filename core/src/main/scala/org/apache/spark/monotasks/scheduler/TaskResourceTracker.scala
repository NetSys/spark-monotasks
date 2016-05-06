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

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.monotasks.{Monotask, MonotaskType}
import org.apache.spark.monotasks.compute.PrepareMonotask

/**
 * Helps track which resources are being used by a particular macrotask.
 *
 * Each instance of this class should correspond to exactly one macrotask. This class handles
 * tracking what monotasks are currently running for the macrotask (to consolidate phase change
 * messages sent to monotasks schedulers).
 *
 * Much of the functionality in this class is to enable the throttling monotasks scheduler (which
 * isn't yet checked in).
 */
private[spark] class TaskResourceTracker extends Logging {

  private class PhaseInfo(val id: Int, var numRunningMonotasks: Int)

  /** Stores information about each resource that has currently running monotasks. */
  private val resourceToInfo = new HashMap[MonotaskType.Value, PhaseInfo]

  /**
   * The id to use for the next phase. Start at 1, because the phase that runs the PrepareMonotask
   * (and gets tasks from the driver) is 0.
   */
  private var nextPhaseId: Int = 1

  /**
   * True if there was just a fork in the monotask DAG, where multiple types of resources were
   * used simultaneously.
   */
  private var previousPhaseForked = false

  /**
   * Updates the tracking about the queues for each type of resource (which determines when the
   * Executor tells the Driver to assign it more tasks).
   *
   * The stageId is the ID of the stage for this task, and should be the same every time
   * updateQueueTracking is called (it's passed in here for convenience, because when the
   * TaskContextImpl is first created, it doesn't have a stage ID set yet).
   *
   * This class handles coalescing monotasks of the same type that run at the same time (or back
   * to back) so that the Monotasks scheduler can deal with fewer phases.
   *
   * TODO: It would be better to pass in a DAG of phases from the LocalDagScheduler. There isn't
   *       quite enough information here to construct the ideal DAG (it's hard to infer information
   *       about branches in the DAG, and in particular, when the branches re-converge).
   */
  def updateQueueTracking(
      stageId: Int,
      completedMonotask: Monotask,
      startedMonotasks: HashSet[Monotask]): Unit = {
    logDebug(s"UpdateQueueTracking called for $completedMonotask ${startedMonotasks.mkString(",")}")
    val completedMonotaskType = MonotaskType.getType(completedMonotask)

    // Try to coalesce monotasks into a single phase if they ran back to back and used the same
    // resource. This is for the benefit of the throttling scheduler.
    val startedMonotaskTypes = startedMonotasks.map(MonotaskType.getType(_))
    if (!previousPhaseForked &&
        !completedMonotask.isInstanceOf[PrepareMonotask] &&
        startedMonotaskTypes.size == 1 &&
        startedMonotaskTypes.contains(completedMonotaskType)) {
      logDebug(s"Skipping update about finished monotask $completedMonotask, because it uses the " +
        "same resource as the previous phase, so can be coalesced to avoid unnecessary " +
        "notifications to MonotasksScheduler")
      return
    }

    // First, determine whether a phase just finished. Do this first (before doing the accounting
    // for newly started phases) to handle DAGs that look like this:
    //
    // Compute --
    //           \
    //            >-----Compute
    //           /
    // Disk -----
    //
    // In this case, we want to make sure the counter for Compute first goes to 0 (so we finish
    // the compute phase) and then the counter increases from 0 to 1 so we start a new compute
    // phase (in other words, we want to make sure not to coalesce the two compute monotasks into
    // a single phase, which would ignore the dependency on the disk monotask).


    // finishedPhaseInfo is an option containing a two-item tuple. It will be None if there isn't a
    // phase that just finished. If the option is set, the first item is a MonotaskType (with the
    // type of the phase that finished) and the second item is the unique id for the phase.
    val finishedPhaseInfo: Option[(MonotaskType.Value, Int)] = {
      if (completedMonotask.isInstanceOf[PrepareMonotask]) {
        // We don't track the PrepareMonotasks as a phase, so we don't need to tell the
        // MonotaskScheduler about them finishing (and we won't have tracked it in
        // resourceToInfo).
        None
      } else {
        assert(resourceToInfo.contains(completedMonotaskType),
          s"Compeleted monotask type $completedMonotaskType expected to be in set of currently " +
            "running resources.")
        var phaseInfo = resourceToInfo(completedMonotaskType)
        phaseInfo.numRunningMonotasks -= 1
        logDebug(s"TaskContextImpl: just finished $completedMonotask; count is " +
          s"${phaseInfo.numRunningMonotasks}")
        assert(phaseInfo.numRunningMonotasks >= 0)
        if (phaseInfo.numRunningMonotasks > 0) {
          // If many monotasks are running for the same resource at the same time, we assume that
          // they all need to complete before future monotasks begin.  For example, we see this
          // with shuffles, where a lot of network monotasks run at the same time, and the compute
          // monotask depends on all of the network monotasks finishing.  That assumption is
          // important because when many monotasks for the same resource start at the same time, we
          // coalesce them into a single phase.
          //
          // Here, a monotask has finished, but there are more monotasks running for the same
          // resource, which means that when the monotasks began, we coalesced them into a single
          // phase. Our code assumes that the next monotasks will depend on the whole phase
          // finishing (othewise, we shouldn't have coalesced the monotasks in the phase together),
          // so here, if some monotasks in the phase are still running, check to make sure no new
          // monotasks have started.
          assert(startedMonotasks.isEmpty)
          None
        } else if (phaseInfo.numRunningMonotasks == 0) {
          resourceToInfo.remove(completedMonotaskType)
          Some((completedMonotaskType, phaseInfo.id))
        } else {
          assert(false, "The number of running monotasks for a resource should not be negative")
          None
        }
      }
    }

    // startedPhaseInfo will be a set of 2-item tuples of phases that started. The first item in
    // each tuple is the type of the phase, and the second item is the unique ID of the phase.
    val startedPhaseInfo = startedMonotasks.flatMap { startedMonotask =>
      val startedMonotaskType = MonotaskType.getType(startedMonotask)
      if (resourceToInfo.contains(startedMonotaskType)) {
        val phaseInfo = resourceToInfo(startedMonotaskType)
        phaseInfo.numRunningMonotasks += 1
        // Coalesce this into the same phase as the other monotasks that are currently running for
        // resource.
        None
      } else {
        val phaseId = nextPhaseId
        nextPhaseId += 1
        logDebug(s"TaskContextImpl: just started $startedMonotaskType with phase id $phaseId")
        resourceToInfo.put(startedMonotaskType, new PhaseInfo(phaseId, 1))
        Some((startedMonotaskType, phaseId))
      }
    }

    if (previousPhaseForked && !startedMonotasks.isEmpty) {
      // Something else is starting -- so assume this means the forked part of the DAG has ended.
      // Here, we're assuming that a forked part of the DAG has at most one monotask in each branch.
      // For example, we're assuming the DAG does *not* look like this (because the top branch has
      // a compute and disk monotask in series):
      //
      //
      //             Disk ---> Compute --
      //            /                     \
      // Compute --<                       >-----Compute
      //            \                     /
      //             ----- Compute -------

      previousPhaseForked = false
    } else {
      previousPhaseForked = startedMonotaskTypes.size > 1
    }

    // Make sure that there's a phase in either finishedPhaseInfo or startedPhaseInfo. If there's
    // not, it means that the monotasks that just finished was in the middle of a phase, so the
    // scheduler doesn't need to be told about it.
    if (!finishedPhaseInfo.isEmpty || !startedPhaseInfo.isEmpty) {
      // Tell the MonotasksScheduler that the macrotask is a new phase, so it can update internal
      // state and request new tasks from the master, as appropriate.
      SparkEnv.get.monotasksScheduler.handlePhaseChange(
        stageId, finishedPhaseInfo, startedPhaseInfo, resourceToInfo.size == 0)
    }
  }
}

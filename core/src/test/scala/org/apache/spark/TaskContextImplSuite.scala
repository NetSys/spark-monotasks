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

package org.apache.spark

import scala.collection.mutable.HashSet

import org.mockito.Mockito.{mock, verify, verifyNoMoreInteractions, when}
import org.scalatest.FunSuite

import org.apache.spark.monotasks.{Monotask, MonotaskType}
import org.apache.spark.monotasks.compute.{ComputeMonotask, PrepareMonotask}
import org.apache.spark.monotasks.disk.{DiskReadMonotask, DiskWriteMonotask}
import org.apache.spark.monotasks.scheduler.MonotasksScheduler

class TaskContextImplSuite extends FunSuite {
  test("updateQueueTracking correctly updates StageQueueTracker") {
    val taskContext = new TaskContextImpl(taskAttemptId = 0, attemptNumber = 0)
    val stageId = 14
    taskContext.initialize(stageId, 0)

    val fakeMonotasksScheduler = mock(classOf[MonotasksScheduler])
    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.monotasksScheduler).thenReturn(fakeMonotasksScheduler)
    SparkEnv.set(sparkEnv)

    // Construct information about the first phase, which does computation.
    val computePhaseInfo = (MonotaskType.Compute, 1)
    val newComputePhase = new HashSet[(MonotaskType.Value, Int)]
    newComputePhase.add(computePhaseInfo)

    // Notify the TaskContextImpl that a prepare monotask has finished and a compute monotask
    // has started. This should cause the TaskContext to tell the queue tracker that a new Compute
    // phase has begun.
    taskContext.updateQueueTracking(
      mock(classOf[PrepareMonotask]),
      HashSet(mock(classOf[ComputeMonotask])))
    verify(fakeMonotasksScheduler).handlePhaseChange(stageId, None, newComputePhase, false)

    // Construct information about the second phase, which does disk.
    val diskPhaseInfo = (MonotaskType.Disk, 2)
    val newDiskPhase = new HashSet[(MonotaskType.Value, Int)]
    newDiskPhase.add(diskPhaseInfo)

    // Notify the TaskContextImpl that the compute monotask has finished and two disk monotasks
    // have started. This should cause the TaskContext to tell the queue tracker that a new Disk
    // phase has begin.
    taskContext.updateQueueTracking(
      mock(classOf[ComputeMonotask]),
      HashSet(mock(classOf[DiskReadMonotask]), mock(classOf[DiskReadMonotask])))
    verify(fakeMonotasksScheduler).handlePhaseChange(
      stageId, Some(computePhaseInfo), newDiskPhase, false)

    // Notify the TaskContextImpl that one of the disk monotasks completed. This shouldn't result
    // in any new interactions with the StageQueueTracker.
    taskContext.updateQueueTracking(
      mock(classOf[DiskReadMonotask]),
      HashSet.empty[Monotask])
    verifyNoMoreInteractions(fakeMonotasksScheduler)

    // Construct information about the first phase, which does computation.
    val secondComputePhaseInfo = (MonotaskType.Compute, 3)
    val secondNewComputePhase = new HashSet[(MonotaskType.Value, Int)]
    secondNewComputePhase.add(secondComputePhaseInfo)

    // When the second disk monotask completes and a new compute monotask begins, the TaskContext
    // should update the StageQueueTracker.
    taskContext.updateQueueTracking(
      mock(classOf[DiskReadMonotask]),
      HashSet(mock(classOf[ComputeMonotask])))
    verify(fakeMonotasksScheduler).handlePhaseChange(
      stageId, Some(diskPhaseInfo), secondNewComputePhase, false)

    // If another compute monotask begins when the previous compute monotask finishes, the task
    // is still in a compute phase, so the StageQueueTracker shouldn't be notified.
    taskContext.updateQueueTracking(
      mock(classOf[ComputeMonotask]),
      HashSet(mock(classOf[ComputeMonotask])))
    verifyNoMoreInteractions(fakeMonotasksScheduler)

    // If the compute monotask finishes and nothing else starts, the StageQueueTracker should be
    // notified that the task has completed.
    taskContext.updateQueueTracking(
      mock(classOf[ComputeMonotask]),
      HashSet.empty[Monotask])
    verify(fakeMonotasksScheduler).handlePhaseChange(
      stageId, Some(secondComputePhaseInfo), new HashSet[(MonotaskType.Value, Int)], true)
  }

  test("updateQueueTracking handles forks in DAG") {
    // Runs a task that first does a compute monotask, then does a compute monotask concurrently
    // with a disk monotask (which is what will happen when, for example, output data is written to
    // disk), and then does a final compute monotask.
    val taskContext = new TaskContextImpl(taskAttemptId = 0, attemptNumber = 0)
    val stageId = 22
    taskContext.initialize(stageId, 0)

    val fakeMonotasksScheduler = mock(classOf[MonotasksScheduler])
    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.monotasksScheduler).thenReturn(fakeMonotasksScheduler)
    SparkEnv.set(sparkEnv)

    // Construct information about the first phase, which does computation.
    val computePhaseInfo = (MonotaskType.Compute, 1)
    val newComputePhase = new HashSet[(MonotaskType.Value, Int)]
    newComputePhase.add(computePhaseInfo)

    // Notify the TaskContextImpl that a prepare monotask has finished and a compute monotask
    // has started. This should cause the TaskContext to tell the queue tracker that a new Compute
    // phase has begun.
    taskContext.updateQueueTracking(
      mock(classOf[PrepareMonotask]),
      HashSet(mock(classOf[ComputeMonotask])))
    verify(fakeMonotasksScheduler).handlePhaseChange(stageId, None, newComputePhase, false)

    // Construct information about the second 2 phases.
    val diskPhaseInfo = (MonotaskType.Disk, 2)
    val secondComputePhaseInfo = (MonotaskType.Compute, 3)
    val branchedNewPhases = new HashSet[(MonotaskType.Value, Int)]
    branchedNewPhases.add(diskPhaseInfo)
    branchedNewPhases.add(secondComputePhaseInfo)

    // Notify the TaskContextImpl that the compute monotask finished, and the disk and compute
    // monotasks have started. The TaskContext should update the StageQueueTracker (and not
    // coalesce the compute monotask into the previous phase, since there's been a fork in the DAG).
    taskContext.updateQueueTracking(
      mock(classOf[ComputeMonotask]),
      HashSet(mock(classOf[DiskWriteMonotask]), mock(classOf[ComputeMonotask])))
    verify(fakeMonotasksScheduler).handlePhaseChange(
      stageId, Some(computePhaseInfo), branchedNewPhases, false)

    // When the compute monotask finishes, the MonotasksScheduler should be updated.
    taskContext.updateQueueTracking(mock(classOf[ComputeMonotask]), HashSet.empty[Monotask])
    verify(fakeMonotasksScheduler).handlePhaseChange(
      stageId,
      Some(secondComputePhaseInfo),
      HashSet.empty[(MonotaskType.Value, Int)],
      false)

    val finalComputePhaseInfo= (MonotaskType.Compute, 4)
    val finalNewComputePhase = HashSet(finalComputePhaseInfo)

    // When the disk monotask finishes and the final compute phase starts, the MonotasksScheduler
    // should again be updated.
    taskContext.updateQueueTracking(
      mock(classOf[DiskWriteMonotask]),
      HashSet(mock(classOf[ComputeMonotask])))
    verify(fakeMonotasksScheduler).handlePhaseChange(
      stageId,
      Some(diskPhaseInfo),
      finalNewComputePhase,
      false)

    // When the last compute phase finishes, the MonotasksScheduler should be updated that the
    // task is done.
    taskContext.updateQueueTracking(
      mock(classOf[ComputeMonotask]),
      HashSet.empty[Monotask])
    verify(fakeMonotasksScheduler).handlePhaseChange(
      stageId,
      Some(finalComputePhaseInfo),
      HashSet.empty[(MonotaskType.Value, Int)],
      true)
  }
}

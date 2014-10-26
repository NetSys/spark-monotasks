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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.HashSet

import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.BlockId

/**
 * A simplified DiskMonotask class that keeps track of how many of its instances are currently
 * being executed and the time at which each instance's execution started. DummyDiskMonotask is a
 * subclass of DiskWriteMonotask so that it does not require a diskId to be known in advance. This
 * class does no meaningful work, so it should only be used for testing purposes.
 */
private[spark] class DummyDiskMonotask(
    localDagScheduler: LocalDagScheduler,
    blockId: BlockId,
    val taskTime: Long)
  extends DiskWriteMonotask(localDagScheduler, blockId, null) {

  override def execute(): Boolean = {
    val taskTimes = DummyDiskMonotask.taskTimes
    taskTimes.put(taskId, System.currentTimeMillis())

    val numRunningTasks = DummyDiskMonotask.numRunningTasks
    val disk = diskId.get
    numRunningTasks.synchronized {
      assert(!numRunningTasks.contains(disk))
      numRunningTasks += disk
    }

    Thread.sleep(taskTime)
    numRunningTasks.synchronized(numRunningTasks.remove(disk))
    true
  }
}

private[spark] object DummyDiskMonotask {
  // Maps diskId to number of DummyDiskMonotasks running on that disk.
  val numRunningTasks = new HashSet[String]()
  // Maps taskId to start time.
  val taskTimes = new ConcurrentHashMap[Long, Long]()

  def clearTimes() = taskTimes.clear()
}

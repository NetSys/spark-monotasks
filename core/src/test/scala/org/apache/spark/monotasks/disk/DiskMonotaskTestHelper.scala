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

package org.apache.spark.monotasks.disk

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.HashMap

/**
 * Singleton object used to track metadata about DiskMonotasks, used only for testing. reset() must
 * be called between test cases.
 */
object DiskMonotaskTestHelper {
  // A mapping from disk ID to the number of tasks that are currently executing on that disk.
  // Updated when executeTaskOnDisk is called to execute a monotask. Used to calculate the values in
  // maxTasksPerDisk and to provide immediate feedback if too many tasks are executed concurrently.
  private val tasksPerDisk = new HashMap[String, Int]()
  // A mapping from disk ID to the maximum number of tasks that have executed concurrently on that
  // disk. Used to ensure that multiple tasks for the same disk are actually being executed
  // concurrently.
  val maxTasksPerDisk = new HashMap[String, Int]()
  // Maps taskId to start time. Entries are added as DiskMonotasks finish so that this can be used
  // to determine whether all of the DiskMonotasks in a test case have finished executing.
  val finishedTaskIdToStartTimeMillis = new ConcurrentHashMap[Long, Long]()

  /**
   * Clears all internal metadata so that this object can be used again.
   */
  def reset(): Unit = {
    tasksPerDisk.clear()
    maxTasksPerDisk.clear()
    finishedTaskIdToStartTimeMillis.clear()
  }

  def executeTaskOnDisk(
      taskId: Long,
      diskId: String,
      taskTimeMillis: Long,
      maxAllowedTasksPerDisk: Int = 1): Unit = {
    val startTimeMillis = System.currentTimeMillis()
    tasksPerDisk.synchronized {
      val numTasksOnDisk = tasksPerDisk.getOrElse(diskId, 0) + 1
      assert(
        numTasksOnDisk <= maxAllowedTasksPerDisk,
        s"Too many tasks running on disk $diskId!" +
          s"actual: $numTasksOnDisk; allowed: $maxAllowedTasksPerDisk")
      tasksPerDisk(diskId) = numTasksOnDisk

      if (numTasksOnDisk > maxTasksPerDisk.getOrElse(diskId, 0)) {
        maxTasksPerDisk(diskId) = numTasksOnDisk
      }
    }

    Thread.sleep(taskTimeMillis)

    tasksPerDisk.synchronized {
      tasksPerDisk(diskId) = tasksPerDisk(diskId) - 1
    }
    finishedTaskIdToStartTimeMillis.put(taskId, startTimeMillis)
  }
}

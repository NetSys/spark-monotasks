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

import scala.collection.mutable.HashSet

/** Singleton object used to track metadata about diskMonotasks, used only for testing. */
object DiskMonotaskTestHelper {
  // The disks where monotasks are currently executing (set when executeTaskOnDisk is called to
  // execute a monotask, and used to ensure only one task is using each disk at a time).
  val disksInUse = new HashSet[String]()
  // Maps taskId to end time.
  val taskTimes = new ConcurrentHashMap[Long, Long]()

  def clearTimes() = taskTimes.clear()

  def executeTaskOnDisk(taskId: Long, diskId: String, taskTime: Long): Unit = {
    disksInUse.synchronized {
      assert(!disksInUse.contains(diskId))
      disksInUse += diskId
    }

    Thread.sleep(taskTime)
    disksInUse.synchronized(disksInUse.remove(diskId))
    taskTimes.put(taskId, System.currentTimeMillis())
  }
}
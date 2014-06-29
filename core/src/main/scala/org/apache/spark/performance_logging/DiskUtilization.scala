/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.performance_logging

import scala.collection.mutable.HashMap

/** Utilization of a particular block device. */
class BlockDeviceUtilization(
    startCounters: BlockDeviceCounters,
    endCounters: BlockDeviceCounters,
    elapsedMillis: Long) extends Serializable {
  val diskUtilization = ((endCounters.millisTotal - startCounters.millisTotal).toFloat /
    elapsedMillis)
  val readThroughput = ((endCounters.sectorsRead - startCounters.sectorsRead).toFloat *
    DiskUtilization.SECTOR_SIZE_BYTES * 1000 / elapsedMillis)
  val writeThroughput = ((endCounters.sectorsWritten - startCounters.sectorsWritten).toFloat *
    DiskUtilization.SECTOR_SIZE_BYTES * 1000 / elapsedMillis)
}

class DiskUtilization(startCounters: DiskCounters, endCounters: DiskCounters) extends Serializable {
  val deviceNameToUtilization = HashMap[String, BlockDeviceUtilization]()
  val elapsedMillis = endCounters.timeMillis - startCounters.timeMillis
  endCounters.deviceNameToCounters.foreach {
    case (deviceName: String, endCounters: BlockDeviceCounters) =>
      startCounters.deviceNameToCounters.get(deviceName).foreach {
        deviceNameToUtilization +=
          deviceName -> new BlockDeviceUtilization(_, endCounters, elapsedMillis)
      }
  }

  def this(startCounters: DiskCounters) = this(startCounters, new DiskCounters())
}

object DiskUtilization {
  // This is not at all portable -- can be obtained for a particular machine with "fdisk -l".
  val SECTOR_SIZE_BYTES = 512
}

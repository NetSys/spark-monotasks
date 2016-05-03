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

package org.apache.spark.performance_logging

import scala.collection.mutable.HashMap

/** Utilization of a particular block device. */
class BlockDeviceUtilization(
    val startCounters: BlockDeviceCounters,
    val endCounters: BlockDeviceCounters,
    val elapsedMillis: Long)
  extends Serializable {

  def diskUtilization: Double =
    (endCounters.millisTotal - startCounters.millisTotal).toDouble / elapsedMillis

  def readThroughput: Double =
    (endCounters.sectorsRead - startCounters.sectorsRead).toDouble *
      DiskUtilization.SECTOR_SIZE_BYTES * 1000 / elapsedMillis

  def writeThroughput: Double =
    (endCounters.sectorsWritten - startCounters.sectorsWritten).toDouble *
      DiskUtilization.SECTOR_SIZE_BYTES * 1000 / elapsedMillis
}

class DiskUtilization(
    val elapsedMillis: Long,
    val deviceNameToUtilization: HashMap[String, BlockDeviceUtilization])
  extends Serializable

object DiskUtilization {
  // This is not at all portable -- can be obtained for a particular machine with "fdisk -l".
  val SECTOR_SIZE_BYTES = 512

  /**
   * Creates a DiskUtilization based on two sets of disk counters.
   *
   * This constructor lives in this companion object because it needs to do some computation
   * (to construct the map of device name to device utilization) before constructing the
   * DiskUtilization object.
   */
  def apply(startCounters: DiskCounters, endCounters: DiskCounters): DiskUtilization = {
    val deviceNameToUtilization = HashMap[String, BlockDeviceUtilization]()
    val elapsedMillis = endCounters.timeMillis - startCounters.timeMillis
    endCounters.deviceNameToCounters.foreach {
      case (deviceName: String, endCounters: BlockDeviceCounters) =>
        startCounters.deviceNameToCounters.get(deviceName).foreach {
          deviceNameToUtilization +=
            deviceName -> new BlockDeviceUtilization(_, endCounters, elapsedMillis)
        }
    }
    new DiskUtilization(elapsedMillis, deviceNameToUtilization)
  }

  def apply(startCounters: DiskCounters): DiskUtilization = apply(startCounters, new DiskCounters())
}

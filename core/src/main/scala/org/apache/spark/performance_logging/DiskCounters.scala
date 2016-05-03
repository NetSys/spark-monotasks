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

import java.io.FileNotFoundException

import scala.collection.mutable.HashMap
import scala.io.Source

import org.apache.spark.Logging

/** Stores counters for a particular block device. */
case class BlockDeviceCounters(val countersLine: String) extends Serializable {
  @transient val items = countersLine.split(" ").filter(!_.isEmpty())
  val deviceName = items(DiskCounters.DEVICE_NAME_INDEX)
  val sectorsRead = items(DiskCounters.SECTORS_READ_INDEX).toLong
  val millisReading = items(DiskCounters.MILLIS_READING_INDEX).toLong
  val sectorsWritten = items(DiskCounters.SECTORS_WRITTEN_INDEX).toLong
  val millisWriting = items(DiskCounters.MILLIS_WRITING_INDEX).toLong
  val millisTotal = items(DiskCounters.MILLIS_TOTAL_INDEX).toLong
}

/** Counters across all block devices. */
class DiskCounters(
    val timeMillis: Long,
    val deviceNameToCounters: HashMap[String, BlockDeviceCounters])
  extends Serializable with Logging {

  def this() = {
    this(System.currentTimeMillis(), new HashMap[String, BlockDeviceCounters]())

    try {
      val totalDiskUseFile = Source.fromFile(DiskCounters.DISK_TOTALS_FILENAME)
      totalDiskUseFile.getLines().foreach { line =>
        if (line.indexOf("loop") == -1 && line.indexOf("ram") == -1) {
          val deviceCounters = BlockDeviceCounters(line)
          this.deviceNameToCounters += deviceCounters.deviceName -> deviceCounters
        }
      }
      totalDiskUseFile.close()
    } catch {
      case e: FileNotFoundException =>
        if (!DiskCounters.emittedMissingFileWarning) {
          logWarning(
            s"Unable to record disk counters because ${DiskCounters.DISK_TOTALS_FILENAME} " +
              "could not be found")
          DiskCounters.emittedMissingFileWarning = true
        }
    }
  }
}

object DiskCounters {
  val DISK_TOTALS_FILENAME = "/proc/diskstats"

  // Indices of information in the DISK_TOTALS_FILENAME file.
  val DEVICE_NAME_INDEX = 2
  val SECTORS_READ_INDEX = 5
  val MILLIS_READING_INDEX = 6
  val SECTORS_WRITTEN_INDEX = 9
  val MILLIS_WRITING_INDEX = 10
  val MILLIS_TOTAL_INDEX = 12

  // Keep track of whether we've emitted a warning that the files in the /proc file system couldn't
  // be found, so we don't output endless warnings.
  var emittedMissingFileWarning = false
}

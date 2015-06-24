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

import scala.io.Source

import org.apache.spark.Logging

class CpuCounters(val timeMillis: Long) extends Serializable with Logging {

  def this() = this(System.currentTimeMillis)

  // Total CPU time used by the Spark process.
  var processUserJiffies = 0L
  var processSystemJiffies = 0L

  // Total CPU time used by all processes on the machine.
  var totalUserJiffies = 0L
  var totalSystemJiffies = 0L

  try {
    val processCpuUseFile = Source.fromFile(s"/proc/${Utils.getPid()}/stat")
    processCpuUseFile.getLines().foreach { line =>
      val values = line.split(" ")
      processUserJiffies = values(CpuCounters.UTIME_INDEX).toLong
      processSystemJiffies = values(CpuCounters.STIME_INDEX).toLong
    }
    processCpuUseFile.close()

    val totalCpuUseFile = Source.fromFile(CpuCounters.CPU_TOTALS_FILENAME)
    totalCpuUseFile.getLines().foreach { line =>
      // Look for only the line that starts with "cpu  ", which has the totals across all CPUs
      // (the remaining lines are for a particular core).
      if (line.startsWith("cpu ")) {
        val cpuTimes =
          line.substring(CpuCounters.CPU_COUNTS_START_INDEX, line.length).split(" ").map(_.toLong)
        totalUserJiffies = cpuTimes(CpuCounters.USER_JIFFIES_INDEX)
        totalSystemJiffies = cpuTimes(CpuCounters.SYSTEM_JIFFIES_INDEX)
      }
    }
    totalCpuUseFile.close()
  } catch {
    case e: FileNotFoundException =>
      if (!CpuCounters.emittedMissingFilesWarning) {
        logWarning(
          "Unable to record CPU counters because files in /proc filesystem could not be found")
        CpuCounters.emittedMissingFilesWarning = true
      }
  }
}

object CpuCounters {
  val CPU_TOTALS_FILENAME = "/proc/stat"

  // The first 5 characters in /proc/stat are "cpu  " or "cpuX " where X is the indentifier of the
  // particular core.
  val CPU_COUNTS_START_INDEX = 5

  // Indexes of the counters for user / system CPU counters, starting after the CPU identifier.
  val USER_JIFFIES_INDEX = 0
  val SYSTEM_JIFFIES_INDEX = 2

  // 0-based index in /proc/pid/stat file of the user CPU time. Not necessarily portable.
  val UTIME_INDEX = 13
  val STIME_INDEX = 14

  // Keep track of whether we've emitted a warning that the files in the /proc file system couldn't
  // be found, so we don't output endless warnings.
  var emittedMissingFilesWarning = false
}

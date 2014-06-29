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

import scala.io.Source

class CpuCounters() extends Serializable {
  val timeMillis = System.currentTimeMillis()

  // Total times used by the Spark process.
  var processUserJiffies: Long = _
  var processSystemJiffies: Long = _
  Source.fromFile(s"/proc/${Utils.getPid()}/stat").getLines().foreach { line =>
    val values = line.split(" ")
    processUserJiffies = values(CpuCounters.UTIME_INDEX).toLong
    processSystemJiffies = values(CpuCounters.STIME_INDEX).toLong
  }

  // Total times across all processes on the machine.
  var totalUserJiffies: Long = _
  var totalSystemJiffies: Long = _
  Source.fromFile(CpuCounters.CPU_TOTALS_FILENAME).getLines().foreach { line =>
    if (line.startsWith("cpu ")) {
      val cpuTimes = line.substring(5, line.length).split(" ").map(_.toInt)
      totalUserJiffies = cpuTimes(0)
      totalSystemJiffies = cpuTimes(2)
    }
  }
}

object CpuCounters {
  val CPU_TOTALS_FILENAME = "/proc/stat"

  // 0-based index in /proc/pid/stat file of the user CPU time. Not necessarily portable.
  val UTIME_INDEX = 13
  val STIME_INDEX = 14
}

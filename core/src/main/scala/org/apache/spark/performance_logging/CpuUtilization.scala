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

/**
 * CPU utilization measures the average number of cores in use, so when the CPUs are fully
 * utilized, the CPU utilization will be equal to the number of cores (i.e., it will typically
 * be greater than 1).
 */
class CpuUtilization(val startCounters: CpuCounters, val endCounters: CpuCounters)
  extends Serializable {

  def elapsedMillis = endCounters.timeMillis - startCounters.timeMillis
  def elapsedJiffies = CpuUtilization.JIFFIES_PER_SECOND * (elapsedMillis * 1.0 / 1000)

  def processUserUtilization =
    ((endCounters.processUserJiffies - startCounters.processUserJiffies).toFloat / elapsedJiffies)
  def processSystemUtilization =
    ((endCounters.processSystemJiffies - startCounters.processSystemJiffies).toFloat /
      elapsedJiffies)
  def totalUserUtilization =
    ((endCounters.totalUserJiffies - startCounters.totalUserJiffies).toFloat / elapsedJiffies)
  def totalSystemUtilization =
    ((endCounters.totalSystemJiffies - startCounters.totalSystemJiffies).toFloat / elapsedJiffies)

  def this(startCounters: CpuCounters) = this(startCounters, new CpuCounters())
}

object CpuUtilization {
  // This is the correct value for most linux systems, and for the default Spark AMI.
  val JIFFIES_PER_SECOND = 100
}

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

import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.util.JsonProtocol

/**
 * Periodically logs information about CPU, network, and disk utilization on the machine to a file.
 */
private[spark] class ContinuousMonitor(
    sparkConf: SparkConf,
    getNumRunningComputeMonotasks: () => Int,
    getNumRunningMacrotasks: () => Int) {
  private val logIntervalMillis = sparkConf.getInt("spark.continuousMonitor.logIntervalMillis", 10)
  val printWriter = new PrintWriter(
    new File(s"/tmp/spark_continuous_monitor_${System.currentTimeMillis}"))

  private var previousGcMillis = Utils.totalGarbageCollectionMillis
  private var previousCpuCounters = new CpuCounters()
  private var previousDiskCounters = new DiskCounters()
  private var previousNetworkCounters = new NetworkCounters()

  private def getUtilizationJson(): JValue = {
    val currentCpuCounters = new CpuCounters()
    val currentDiskCounters = new DiskCounters()
    val currentNetworkCounters = new NetworkCounters()
    val currentGcMillis = Utils.totalGarbageCollectionMillis

    val cpuUtilization = new CpuUtilization(previousCpuCounters, currentCpuCounters)
    val diskUtilization = DiskUtilization(previousDiskCounters, currentDiskCounters)
    val networkUtilization = new NetworkUtilization(previousNetworkCounters, currentNetworkCounters)
    val elapsedMillis = currentCpuCounters.timeMillis - previousCpuCounters.timeMillis
    val fractionGcTime = (currentGcMillis - previousGcMillis).toDouble / elapsedMillis

    previousCpuCounters = currentCpuCounters
    previousDiskCounters = currentDiskCounters
    previousNetworkCounters = currentNetworkCounters
    previousGcMillis = currentGcMillis

    ("Current Time" -> currentCpuCounters.timeMillis) ~
    ("Previous Time" -> previousCpuCounters.timeMillis) ~
    ("Fraction GC Time" -> fractionGcTime) ~
    ("Cpu Utilization" -> JsonProtocol.cpuUtilizationToJson(cpuUtilization)) ~
    ("Disk Utilization" -> JsonProtocol.diskUtilizationToJson(diskUtilization)) ~
    ("Network Utilization" -> JsonProtocol.networkUtilizationToJson(networkUtilization)) ~
    ("Running Compute Monotasks" -> getNumRunningComputeMonotasks()) ~
    ("Running Macrotasks" -> getNumRunningMacrotasks())
  }

  def start(env: SparkEnv) {
    import env.actorSystem.dispatcher
    // TODO: Don't bother starting this when the /proc filesystem isn't available on the machine.
    env.actorSystem.scheduler.schedule(
      Duration(0, TimeUnit.MILLISECONDS),
      Duration(logIntervalMillis, TimeUnit.MILLISECONDS)) {
      // TODO: Will this interfere with other uses of the disk? Should write to different disk?
      //       To the EBS volume? Experiments so far suggest no (because the volume of data is
      //       small), but worth keeping an eye on.
      printWriter.write(JsonMethods.compact(JsonMethods.render(getUtilizationJson())))
    }
  }

  def stop() {
    printWriter.close()
  }
}

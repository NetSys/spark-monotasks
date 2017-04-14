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

import scala.collection.mutable.HashMap
import scala.concurrent.duration.Duration

import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.util.JsonProtocol

/**
 * Periodically logs information about CPU, network, and disk utilization on the machine to a file.
 */
private[spark] class ContinuousMonitor(
    sparkConf: SparkConf,
    getOutstandingNetworkBytes: () => Long,
    getNumRunningComputeMonotasks: () => Int,
    getNumRunningPrepareMonotasks: () => Int,
    getDiskNameToNumRunningAndQueuedDiskMonotasks: () => HashMap[String, (Int, Int, Int, Int)],
    getNumRunningMacrotasks: () => Int,
    getNumLocalRunningMacrotasks: () => Int,
    getNumMacrotasksInCompute: () => Long,
    getNumMacrotasksInDisk: () => Long,
    getNumMacrotasksInNetwork: () => Long,
    getFreeHeapMemoryBytes: () => Long,
    getFreeOffHeapMemory: () => Long) {
  private val logIntervalMillis = sparkConf.getInt("spark.continuousMonitor.logIntervalMillis", 10)
  val printWriter = new PrintWriter(
    new File(s"/tmp/spark_continuous_monitor_${System.currentTimeMillis}"))

  private var previousGcMillis = Utils.totalGarbageCollectionMillis
  private var previousCpuCounters = new CpuCounters()
  private var previousDiskCounters = new DiskCounters()
  private var previousNetworkCounters = new NetworkCounters()

  private def getDiskNameToCountsJson(
      diskNameToCount: HashMap[String, (Int, Int, Int, Int)]): JValue = {
    JArray(diskNameToCount.toList.map {
      case (name, count) =>
        ("Disk Name" -> name) ~
        ("Running And Queued Monotasks" -> count._1) ~
        ("Queued Read Monotasks" -> count._2) ~
        ("Queued Remove Monotasks" -> count._3) ~
        ("Queued Write Monotasks" -> count._4)
    })
  }

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
    ("Outstanding Network Bytes" -> getOutstandingNetworkBytes()) ~
    ("Running Compute Monotasks" -> getNumRunningComputeMonotasks()) ~
    ("Running Prepare Monotasks" -> getNumRunningPrepareMonotasks()) ~
    ("Running Disk Monotasks" ->
      getDiskNameToCountsJson(getDiskNameToNumRunningAndQueuedDiskMonotasks())) ~
    ("Running Macrotasks" -> getNumRunningMacrotasks()) ~
    ("Local Running Macrotasks" -> getNumLocalRunningMacrotasks()) ~
    ("Macrotasks In Compute" -> getNumMacrotasksInCompute()) ~
    ("Macrotasks In Disk" -> getNumMacrotasksInDisk()) ~
    ("Macrotasks In Network" -> getNumMacrotasksInNetwork()) ~
    ("Free Heap Memory Bytes" -> getFreeHeapMemoryBytes()) ~
    ("Free Off-Heap Memory Bytes" -> getFreeOffHeapMemory())
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
      printWriter.write("\n")
    }
  }

  def stop() {
    printWriter.close()
  }
}

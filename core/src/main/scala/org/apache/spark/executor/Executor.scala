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

package org.apache.spark.executor

import java.nio.ByteBuffer

import scala.util.control.NonFatal

import akka.actor.Props

import org.apache.spark._
import org.apache.spark.monotasks.SubmitMonotask
import org.apache.spark.monotasks.compute.PrepareMonotask
import org.apache.spark.util.{SparkUncaughtExceptionHandler, AkkaUtils, Utils}
import org.apache.spark.performance_logging.ContinuousMonitor

/**
 * Spark executor used with Mesos, YARN, and the standalone scheduler.
 * In coarse-grained mode, an existing actor system is provided.
 */
private[spark] class Executor(
    private val executorId: String,
    executorHostname: String,
    executorBackend: ExecutorBackend,
    private val env: SparkEnv,
    isLocal: Boolean = false)
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  private val conf = env.conf

  @volatile private var isStopped = false

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(SparkUncaughtExceptionHandler)
  }

  val executorSource = new ExecutorSource(this, executorId)

  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    env.blockManager.initialize(conf.getAppId)
  }

  // Create an actor for receiving RPCs from the driver
  private val executorActor = env.actorSystem.actorOf(
    Props(new ExecutorActor(executorId)), "ExecutorActor")

  private val localDagScheduler = env.localDagScheduler
  localDagScheduler.initialize(executorBackend, env.blockManager.memoryStore)

  private val continuousMonitor = new ContinuousMonitor(
    conf,
    localDagScheduler.getOutstandingNetworkBytes,
    localDagScheduler.getNumRunningComputeMonotasks,
    localDagScheduler.getNumRunningPrepareMonotasks,
    localDagScheduler.getDiskNameToNumRunningAndQueuedDiskMonotasks,
    localDagScheduler.getNumRunningMacrotasks,
    localDagScheduler.getNumLocalRunningMacrotasks,
    localDagScheduler.getNumMacrotasksInCompute,
    localDagScheduler.getNumMacrotasksInDisk,
    localDagScheduler.getNumMacrotasksInNetwork,
    () => env.blockManager.memoryStore.freeHeapMemory,
    () => env.blockManager.memoryStore.freeOffHeapMemory)
  continuousMonitor.start(env)

  startDriverHeartbeater()

  def launchTask(
      taskAttemptId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer) {
    val context = new TaskContextImpl(
      taskAttemptId,
      attemptNumber)
    val prepareMonotask = new PrepareMonotask(context, serializedTask)
    localDagScheduler.post(SubmitMonotask(prepareMonotask))
  }

  def killTask(taskId: Long, interruptThread: Boolean) {
    // TODO: Support killing tasks: https://github.com/NetSys/spark-monotasks/issues/4
  }

  def startDriverHeartbeater() {
    val interval = conf.getInt("spark.executor.heartbeatInterval", 10000)
    val timeout = AkkaUtils.lookupTimeout(conf)
    val retryAttempts = AkkaUtils.numRetries(conf)
    val retryIntervalMs = AkkaUtils.retryWaitMs(conf)
    val heartbeatReceiverRef = AkkaUtils.makeDriverRef("HeartbeatReceiver", conf, env.actorSystem)

    val t = new Thread() {
      override def run() {
        // Sleep a random interval so the heartbeats don't end up in sync
        Thread.sleep(interval + (math.random * interval).asInstanceOf[Int])

        while (!isStopped) {
          // TODO: Include task metrics updates here. Currently, the only purpose of this
          //       heartbeat is as a block manager heartbeat, so that the driver knows that the
          //       block manager on this machine is still alive.
          //       https://github.com/NetSys/spark-monotasks/issues/6
          val message = Heartbeat(
            executorId, Array.empty[(Long, TaskMetrics)], env.blockManager.blockManagerId)
          try {
            val response = AkkaUtils.askWithReply[HeartbeatResponse](message, heartbeatReceiverRef,
              retryAttempts, retryIntervalMs, timeout)
            if (response.reregisterBlockManager) {
              logWarning("Told to re-register on heartbeat")
              env.blockManager.reregister()
            }
          } catch {
            case NonFatal(t) => logWarning("Issue communicating with driver in heartbeater", t)
          }

          Thread.sleep(interval)
        }
      }
    }
    t.setDaemon(true)
    t.setName("Driver Heartbeater")
    t.start()
  }

  def stop() {
    env.metricsSystem.report()
    env.actorSystem.stop(executorActor)
    continuousMonitor.stop()
    isStopped = true
  }
}

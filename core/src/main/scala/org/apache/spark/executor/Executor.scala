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

import org.apache.spark.{Logging, SparkConf, SparkEnv, TaskContext, TaskState}
import org.apache.spark.util.{AkkaUtils, Utils}
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.monotasks.compute.PrepareMonotask

/**
 * Spark executor used with Mesos, YARN, and the standalone scheduler.
 */
private[spark] class Executor(
    executorId: String,
    slaveHostname: String,
    executorBackend: ExecutorBackend,
    properties: Seq[(String, String)],
    isLocal: Boolean = false)
  extends Logging
{
  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  @volatile private var isStopped = false

  // No ip or host:port - just hostname
  Utils.checkHost(slaveHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(slaveHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(slaveHostname)

  // Set spark.* properties from executor arg
  val conf = new SparkConf(true)
  conf.setAll(properties)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(ExecutorUncaughtExceptionHandler)
  }

  val executorSource = new ExecutorSource(this, executorId)

  // Initialize Spark environment (using system properties read above)
  private val env = {
    if (!isLocal) {
      val _env = SparkEnv.create(conf, executorId, slaveHostname, 0,
        isDriver = false, isLocal = false)
      SparkEnv.set(_env)
      _env.metricsSystem.registerSource(executorSource)
      _env
    } else {
      SparkEnv.get
    }
  }

  // Create our DependencyManager, which manages the class loader.
  private val dependencyManager = new DependencyManager(env, conf)

  // If a task result is larger than this, we use the block manager to send the task result back.
  private val maximumResultSizeBytes =
    AkkaUtils.maxFrameSizeBytes(conf) - AkkaUtils.reservedSizeBytes

  private val localDagScheduler = new LocalDagScheduler(executorBackend, env.blockManager)

  def launchTask(taskAttemptId: Long, taskName: String, serializedTask: ByteBuffer) {
    // TODO: Do we really need to propogate this task started message back to the scheduler?
    //       Doesn't the scheduler just drop it?
    executorBackend.statusUpdate(taskAttemptId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
    val context = new TaskContext(
      env, localDagScheduler, maximumResultSizeBytes, dependencyManager, taskAttemptId)
    val prepareMonotask = new PrepareMonotask(context, serializedTask)
    localDagScheduler.submitMonotask(prepareMonotask)
  }

  def killTask(taskId: Long, interruptThread: Boolean) {
    // TODO: Support killing tasks: https://github.com/NetSys/spark-monotasks/issues/4
  }

  def stop() {
    env.metricsSystem.report()
    isStopped = true
  }
}

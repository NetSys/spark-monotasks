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

package org.apache.spark.monotasks.compute

import java.nio.ByteBuffer

import org.apache.spark.{SparkEnv, TaskContextImpl}
import org.apache.spark.monotasks.SubmitMonotasks
import org.apache.spark.scheduler.Macrotask

/**
 * A ComputeMonotask responsible for preparing the rest of the monotasks corresponding to the
 * macrotask (e.g., by first deserializing the byte buffer to determine what kind of macro
 * task this is).
 */
private[spark] class PrepareMonotask(context: TaskContextImpl, val serializedTask: ByteBuffer)
  extends ComputeMonotask(context) {

  override def execute(): Option[ByteBuffer] = {
    context.taskMetrics.setStartNetworkTransmitTotalIdleMillis(
      SparkEnv.get.localDagScheduler.getNetworkTransmitTotalIdleMillis())

    val (taskFiles, taskJars, taskBytes) = Macrotask.deserializeWithDependencies(serializedTask)

    val dependencyManager = SparkEnv.get.dependencyManager
    // TODO: This call is a little bit evil because it's synchronized, so can block and waste CPU
    //       resources.
    dependencyManager.updateDependencies(taskFiles, taskJars)

    val deserializationStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()

    val macrotask = ser.deserialize[Macrotask[Any]](taskBytes, dependencyManager.replClassLoader)
    context.taskMetrics.setExecutorDeserializeTime(
      System.currentTimeMillis() - deserializationStartTime)

    context.initialize(macrotask.stageId, macrotask.partition.index)

    SparkEnv.get.mapOutputTracker.updateEpoch(macrotask.epoch)

    SparkEnv.get.localDagScheduler.post(SubmitMonotasks(macrotask.getMonotasks(context)))
    None
  }
}

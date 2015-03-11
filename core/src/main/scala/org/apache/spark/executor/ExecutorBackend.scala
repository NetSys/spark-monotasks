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

package org.apache.spark.executor

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState

/**
 * A pluggable interface used by the Executor to send updates to the cluster scheduler.
 */
private[spark] trait ExecutorBackend {
  def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer)

  /**
   * Notifies the driver about how many free cores are available on the machine. A negative value
   * implies that there is currently a queue of compute monotasks on the machine (e.g., -5 means
   * that 5 compute monotasks are currently waiting to be executed).
   */
  def updateFreeCores(cores: Int): Unit = {
    // This method is implemented but throws an exception to avoid compiler errors for types of
    // executor backends that don't currently support this method.
    // TODO: Implement this method in all subclasses of ExecutorBackend.
    //       https://github.com/NetSys/spark-monotasks/issues/19
    throw new Exception("ExecutorBackends must implement updateFreeCores()")
  }
}


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

package org.apache.spark

import scala.collection.mutable.{ArrayBuffer, Map}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.{TaskCompletionListener, TaskCompletionListenerException}

/**
 * Stores metadata about a macrotask, including metrics.
 *
 * @param runningLocally Whether the task is running in the same JVM as the Spark Master.
 * @param remoteName If the TaskContextImpl is for a macrotask that is running on a remote executor,
 *                   the name of that executor.
 */
private[spark] class TaskContextImpl(
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    val runningLocally: Boolean = false,
    val remoteName: String = "localhost",
    val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  /* stageId, partitionId, and accumulators are set in initialize() (which is called after the
   * macrotask is deserialized on a executor). */
  var stageId: Int = -1
  var partitionId: Int = -1

  // The accumulators used by this macrotask (passed back to the driver when the task completes).
  var accumulators = Map[Long, Accumulable[_, _]]()

  // For backwards-compatibility; this method is now deprecated as of 1.3.0.
  override def attemptId(): Long = taskAttemptId

  // List of callback functions to execute when the task completes.
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  // Whether the corresponding task has been killed.
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  @volatile private var completed: Boolean = false

  def taskIsRunningRemotely: Boolean = !(remoteName.equals("localhost"))

  def initialize(stageId: Int, partitionId: Int) {
    this.stageId = stageId
    this.partitionId = partitionId
  }

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  override def addTaskCompletionListener(f: TaskContext => Unit): this.type = {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    }
    this
  }

  @deprecated("use addTaskCompletionListener", "1.1.0")
  override def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f()
    }
  }

  /** Marks the task as completed and triggers the listeners. */
  private[spark] def markTaskCompleted(): Unit = {
    completed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process complete callbacks in the reverse order of registration
    onCompleteCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskCompletion(this)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskCompletionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs)
    }
  }

  /** Marks the task for interruption, i.e. cancellation. */
  private[spark] def markInterrupted(): Unit = {
    interrupted = true
  }

  override def isCompleted(): Boolean = completed

  override def isRunningLocally(): Boolean = runningLocally

  override def isInterrupted(): Boolean = interrupted
}


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

package org.apache.spark.monotasks

import java.nio.ByteBuffer

import org.apache.spark.TaskContextImpl
import org.apache.spark.monotasks.compute.ComputeMonotask

/** A minimal subclass of monotask for use in testing. */
class SimpleMonotask(context: TaskContextImpl) extends ComputeMonotask(context) {

  def this(taskAttemptId: Long) = this(new TaskContextImpl(taskAttemptId, 0))

  override def execute(): Option[ByteBuffer] = None

  override def executeAndHandleExceptions(): Unit = {}
}

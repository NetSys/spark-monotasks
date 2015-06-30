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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkEnv, TaskContextImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{MonotaskResultBlockId, StorageLevel}

/**
 * Monotask that handles executing the core computation of a macrotask. The result is stored in
 * memory.
 */
private[spark] abstract class ExecutionMonotask[T, U: ClassTag](
    context: TaskContextImpl,
    val rdd: RDD[T],
    val split: Partition)
  extends ComputeMonotask(context) {

  resultBlockId = Some(new MonotaskResultBlockId(taskId))

  /** Subclasses should define this to return a macrotask result to be sent to the driver. */
  def getResult(): U

  override protected def execute(): Option[ByteBuffer] = {
    SparkEnv.get.blockManager.cacheSingle(
      getResultBlockId(), getResult(), StorageLevel.MEMORY_ONLY, false)
    None
  }
}

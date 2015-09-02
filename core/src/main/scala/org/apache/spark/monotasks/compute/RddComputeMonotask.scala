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

import org.apache.spark.{Partition, SparkEnv, TaskContextImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{MonotaskResultBlockId, StorageLevel}

/**
 * Computes the specified partition of the specified RDD and stores the result in the BlockManager.
 * An RddComputeMonotask only needs to be used when the result of computing the RDD partition needs
 * to be stored on disk.
 */
private[spark] class RddComputeMonotask[T](context: TaskContextImpl, rdd: RDD[T], split: Partition)
  extends ComputeMonotask(context) {

  resultBlockId = Some(new MonotaskResultBlockId(taskId, SparkEnv.get.blockManager.compressRdds))

  override def execute(): Option[ByteBuffer] = {
    // Pass "updateMetrics = false" because blocks cached with MonotaskResultBlockIds are
    // intermediate data that is deleted by the time that the macrotask completes.
    rdd.cacheRdd(
      rdd.compute(split, context),
      split,
      getResultBlockId(),
      context,
      StorageLevel.MEMORY_ONLY,
      updateMetrics = false)
    None
  }
}

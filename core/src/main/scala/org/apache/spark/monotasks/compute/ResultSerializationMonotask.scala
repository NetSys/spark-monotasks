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

import org.apache.spark.{Accumulators, Logging, TaskContextImpl}
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult}
import org.apache.spark.storage.{BlockId, StorageLevel, TaskResultBlockId}

/**
 * ResultSerializationMonotasks are responsible for serializing the result of a macrotask and the
 * associated metrics. The DAG for a macrotask always contains exactly one
 * ResultSerializationMonotask, and it is run after all of the macrotask's other monotasks have
 * completed (because otherwise the metrics computed by ResultSerializationMonotask would not be
 * complete).
 */
class ResultSerializationMonotask(context: TaskContextImpl, resultBlockId: BlockId)
  extends ComputeMonotask(context) with Logging {

  override def execute(): Option[ByteBuffer] = {
    val blockManager = context.localDagScheduler.blockManager
    blockManager.getSingle(resultBlockId).map { result =>
      blockManager.removeBlockFromMemory(resultBlockId, false)
      context.markTaskCompleted()

      // The mysterious choice of which serializer to use when is written to be consistent with
      // Spark.
      val closureSerializer = context.env.closureSerializer.newInstance()
      val resultSer = context.env.serializer.newInstance()

      val serializationStartTime = System.currentTimeMillis()
      val valueBytes = resultSer.serialize(result)
      context.taskMetrics.setResultSerializationTime(
        System.currentTimeMillis() - serializationStartTime)
      accountForComputeTime()

      context.taskMetrics.setMetricsOnTaskCompletion()
      val accumulatorValues = Accumulators.getValues
      val directResult = new DirectTaskResult(valueBytes, accumulatorValues, context.taskMetrics)
      val serializedDirectResult = closureSerializer.serialize(directResult)
      val resultSize = serializedDirectResult.limit

      if (context.maximumResultSizeBytes > 0 && resultSize > context.maximumResultSizeBytes) {
        val blockId = TaskResultBlockId(context.taskAttemptId)
        context.localDagScheduler.blockManager.cacheBytes(
          blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
        logInfo(s"Finished TID ${context.taskAttemptId}. $resultSize bytes result will be sent " +
          "via the BlockManager.")
        closureSerializer.serialize(new IndirectTaskResult[Any](blockId, resultSize))
      } else {
        logInfo(s"Finished TID ${context.taskAttemptId}. $resultSize bytes result will be sent " +
          "directly to driver.")
        serializedDirectResult
      }
    }.orElse {
      throw new IllegalStateException(s"Deserialized result for macrotask " +
        s"${context.taskAttemptId} could not be found in the BlockManager " +
        s"using blockId $resultBlockId.")
    }
  }
}

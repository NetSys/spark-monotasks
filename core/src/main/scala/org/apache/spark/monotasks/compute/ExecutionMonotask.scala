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

import org.apache.spark.{Accumulators, Logging, Partition, TaskContextImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult}
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}

/**
 * Monotask that handles executing the core computation of a macro task and serializing the result.
 */
private[spark] abstract class ExecutionMonotask[T, U: ClassTag](
    context: TaskContextImpl,
    val rdd: RDD[T],
    val split: Partition)
  extends ComputeMonotask(context) with Logging {

  /** Subclasses should define this to return a macrotask result to be sent to the driver. */
  def getResult(): U

  override protected def execute(): Option[ByteBuffer] = {
    val result = getResult()
    context.markTaskCompleted()
    val serializedResult = serializeResult(result)
    Some(serializedResult)
  }

  private def serializeResult(result: U): ByteBuffer = {
    // The mysterious choice of which serializer to use when is written to be consistent with Spark.
    val closureSerializer = context.env.closureSerializer.newInstance()
    val resultSer = context.env.serializer.newInstance()

    val serializationStartTime = System.currentTimeMillis()
    val valueBytes = resultSer.serialize(result)
    context.taskMetrics.setResultSerializationTime(
      System.currentTimeMillis() - serializationStartTime)

    context.taskMetrics.setMetricsOnTaskCompletion()
    val accumulatorValues = Accumulators.getValues
    val directResult = new DirectTaskResult(valueBytes, accumulatorValues, context.taskMetrics)
    val serializedDirectResult = closureSerializer.serialize(directResult)
    val resultSize = serializedDirectResult.limit

    if (context.maximumResultSizeBytes > 0 && resultSize > context.maximumResultSizeBytes) {
      val blockId = TaskResultBlockId(context.taskAttemptId)
      context.env.blockManager.putBytes(
        blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
      logInfo(s"Finished TID ${context.taskAttemptId}. $resultSize bytes result will be sent " +
        "via the BlockManager)")
      closureSerializer.serialize(new IndirectTaskResult[Any](blockId, resultSize))
    } else {
      logInfo(s"Finished TID ${context.taskAttemptId}. $resultSize bytes result will be sent " +
        "directly to driver")
      serializedDirectResult
    }
  }
}

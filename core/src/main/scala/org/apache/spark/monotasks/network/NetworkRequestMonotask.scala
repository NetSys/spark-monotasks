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

package org.apache.spark.monotasks.network

import scala.util.control.NonFatal

import org.apache.spark.{FetchFailed, Logging, SparkEnv, SparkException, TaskContextImpl}
import org.apache.spark.monotasks.TaskSuccess
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.BlockReceivedCallback
import org.apache.spark.storage.{BlockId, BlockManagerId, MonotaskResultBlockId, ShuffleBlockId,
  StorageLevel}
import org.apache.spark.util.Utils

/**
 * A monotask that uses the network to request shuffle data.  This monotask handles only fetching
 * data, and does not deserialize it.
 *
 * @param remoteAddress remote BlockManager to fetch from.
 * @param shuffleBlockId Id of the remote block to fetch.
 * @param size Estimated size of the data to fetch (used to keep track of how many bytes are in
 *             flight).
 */
private[spark] class NetworkRequestMonotask(
    context: TaskContextImpl,
    private val remoteAddress: BlockManagerId,
    private val shuffleBlockId: ShuffleBlockId,
    private val size: Long)
  extends NetworkMonotask(context) with Logging with BlockReceivedCallback {

  resultBlockId = Some(new MonotaskResultBlockId(taskId))

  /** Scheduler to notify about bytes received over the network. Set by execute(). */
  var networkScheduler: Option[NetworkScheduler] = None

  override def execute(scheduler: NetworkScheduler): Unit = {
    logInfo(s"Sending request for block $shuffleBlockId (size (${Utils.bytesToString(size)}}) " +
      s"to $remoteAddress")
    networkScheduler = Some(scheduler)
    scheduler.addOutstandingBytes(size)

    try {
      SparkEnv.get.blockTransferService.fetchBlock(
        remoteAddress.host,
        remoteAddress.port,
        shuffleBlockId.toString,
        this)
    } catch {
      case NonFatal(t) => {
        logError(s"Failed to initiate fetchBlock for shuffle block $shuffleBlockId", t)
        val failureReason = FetchFailed(
          remoteAddress,
          shuffleBlockId.shuffleId,
          shuffleBlockId.mapId,
          context.partitionId,
          Utils.exceptionString(t))
        handleException(failureReason)
      }
    }
  }

  override def onSuccess(blockId: String, buf: ManagedBuffer): Unit = {
    networkScheduler.map(_.addOutstandingBytes(-size)).orElse {
      throw new IllegalStateException(
        s"onSuccess called for block $blockId in monotask $taskId before a NetworkScheduler " +
        "was configured")
    }

    // Increment the ref count because we need to pass this to a different thread.
    // This needs to be released after use.
    buf.retain()
    SparkEnv.get.blockManager.cacheSingle(getResultBlockId(), buf, StorageLevel.MEMORY_ONLY, false)
    localDagScheduler.post(TaskSuccess(this))
  }

  override def onFailure(failedBlockId: String, e: Throwable): Unit = {
    networkScheduler.map(_.addOutstandingBytes(-size)).orElse {
      throw new IllegalStateException(
        s"onFailure called for block $failedBlockId in monotask $taskId before a " +
        "NetworkScheduler was configured")
    }

    logError(s"Failed to get block(s) from ${remoteAddress.host}:${remoteAddress.port}", e)
    BlockId(failedBlockId) match {
      case ShuffleBlockId(shuffleId, mapId, _) =>
        val failureReason = FetchFailed(
          remoteAddress,
          shuffleId.toInt,
          mapId,
          context.partitionId,
          Utils.exceptionString(e))
        handleException(failureReason)

      case _ =>
        val exception = new SparkException(
          s"Failed to get block $failedBlockId, which is not a shuffle block")
        handleException(exception)
    }
  }
}

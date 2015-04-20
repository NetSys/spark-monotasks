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

import org.apache.spark.{ExceptionFailure, FetchFailed, Logging, SparkException, TaskContextImpl}
import org.apache.spark.monotasks.Monotask
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage.{BlockId, BlockManagerId, MonotaskResultBlockId, ShuffleBlockId}
import org.apache.spark.util.Utils

/**
 * A monotask that uses the network to fetch shuffle data.  This monotask handles only fetching
 * data, and does not deserialize it.
 *
 * @param remoteAddress remote BlockManager to fetch from.
 * @param shuffleBlockId Id of the remote block to fetch.
 * @param size Estimated size of the data to fetch (used to keep track of how many bytes are in
 *             flight).
 */
private[spark] class NetworkMonotask(
    context: TaskContextImpl,
    val remoteAddress: BlockManagerId,
    shuffleBlockId: ShuffleBlockId,
    size: Long)
  extends Monotask(context) with Logging with BlockFetchingListener {

  val resultBlockId = new MonotaskResultBlockId(taskId)

  def execute() {
    logDebug(s"Sending request for block $shuffleBlockId (size (${Utils.bytesToString(size)}}) " +
      s"to $remoteAddress")
    context.env.blockManager.blockTransferService.fetchBlocks(
      remoteAddress.host,
      remoteAddress.port,
      remoteAddress.executorId,
      Array(shuffleBlockId.toString),
      this)
  }

  override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
    // Increment the ref count because we need to pass this to a different thread.
    // This needs to be released after use.
    buf.retain()
    context.env.blockManager.memoryStore.putValue(resultBlockId, buf)
    context.localDagScheduler.handleTaskCompletion(this)
  }

  override def onBlockFetchFailure(failedBlockId: String, e: Throwable): Unit = {
    logError(s"Failed to get block(s) from ${remoteAddress.host}:${remoteAddress.port}", e)
    val serializedFailureReason = BlockId(failedBlockId) match {
      case ShuffleBlockId(shuffleId, mapId, _) =>
        val failureReason = FetchFailed(
          remoteAddress,
          shuffleId.toInt,
          mapId,
          context.partitionId,
          Utils.exceptionString(e))
        context.env.closureSerializer.newInstance().serialize(failureReason)

      case _ =>
        val exception = new SparkException(
          s"Failed to get block $failedBlockId, which is not a shuffle block")
        val reason = new ExceptionFailure(exception, Some(context.taskMetrics))
        context.env.closureSerializer.newInstance().serialize(reason)
    }
    context.localDagScheduler.handleTaskFailure(this, serializedFailureReason)
  }
}

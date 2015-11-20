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

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.apache.spark.{FetchFailed, Logging, SparkEnv, SparkException, TaskContextImpl}
import org.apache.spark.monotasks.TaskSuccess
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.BlockReceivedCallback
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * A monotask that uses the network to request shuffle data.  This monotask handles only fetching
 * data, and does not deserialize it.
 *
 * @param remoteAddress remote BlockManager to fetch from.
 * @param shuffleBlockIdsAndSizes sequence of two-item tuples describing the shuffle blocks, where
 *                                the first item is a BlockId and the second item is the block's
 *                                approximate size.
 */
private[spark] class NetworkRequestMonotask(
    context: TaskContextImpl,
    private val remoteAddress: BlockManagerId,
    private val shuffleBlockIdsAndSizes: Seq[(ShuffleBlockId, Long)])
  extends NetworkMonotask(context) with Logging with BlockReceivedCallback {

  /** Scheduler to notify about bytes received over the network. Set by execute(). */
  private var networkScheduler: Option[NetworkScheduler] = None

  /** Time when the request for remote data was issued. */
  private var requestIssueTimeNanos = 0L

  val outstandingBlockIdToSize = new HashMap[BlockId, Long]
  shuffleBlockIdsAndSizes.foreach {
    case (shuffleBlockId, size) =>
      outstandingBlockIdToSize.put(shuffleBlockId, size)
  }

  override def execute(scheduler: NetworkScheduler): Unit = {
    val blockIds = shuffleBlockIdsAndSizes.map(_._1)
    val blockIdsAsString = blockIds.mkString(", ")
    val totalSize = shuffleBlockIdsAndSizes.map(_._2).sum
    logInfo(s"Monotask $taskId: sending request for blocks $blockIdsAsString (total size " +
      s"(${Utils.bytesToString(totalSize)}}) to $remoteAddress")
    networkScheduler = Some(scheduler)
    scheduler.addOutstandingBytes(totalSize)
    requestIssueTimeNanos = System.nanoTime()

    try {
      SparkEnv.get.blockTransferService.fetchBlocks(
        remoteAddress.host,
        remoteAddress.port,
        blockIds.map(_.toString).toArray,
        context.taskAttemptId,
        context.attemptNumber,
        this)
    } catch {
      case NonFatal(t) => {
        logError(s"Failed to initiate fetchBlock for shuffle blocks $blockIdsAsString", t)
        // Use the first shuffle block as the failure reason (only one fetch failure will be
        // reported back to the master anyway).
        val failureReason = FetchFailed(
          remoteAddress,
          blockIds(0).shuffleId,
          blockIds(0).mapId,
          context.partitionId,
          Utils.exceptionString(t))
        handleException(failureReason)
      }
    }
  }

  /**
   * Removes the given block from outstandingBlockIdToSize (which is used to determine when this
   * monotask has finished), and notifies the NetworkScheduler that the bytes in the given block are
   * no longer outstanding.
   */
  private def removeOutstandingBlock(blockIdStr: String, callbackMethodName: String): Unit = {
    val blockId = BlockId(blockIdStr)
    val size = outstandingBlockIdToSize.getOrElse(blockId,
      throw new IllegalStateException(
        s"$callbackMethodName called for block $blockId, which is not outstanding"))

    networkScheduler.map(_.addOutstandingBytes(-size)).orElse {
      throw new IllegalStateException(
        s"$callbackMethodName called for block $blockId in monotask $taskId before a " +
          s"NetworkScheduler was configured")
    }
    outstandingBlockIdToSize -= blockId
  }

  /** This method will be called multiple times, one for each block that was requested. */
  override def onSuccess(
      blockId: String,
      diskReadNanos: Long,
      totalRemoteNanos: Long,
      buf: ManagedBuffer): Unit = {
    removeOutstandingBlock(blockId, "onSuccess")

    val elapsedNanos = System.nanoTime() - requestIssueTimeNanos
    logInfo(s"Received block $blockId from BlockManagerId $remoteAddress (total elapsed nanos: " +
      s"$elapsedNanos; nanos on remote machine: $totalRemoteNanos; disk nanos: $diskReadNanos)")

    // Increment the ref count because we need to pass this to a different thread.
    // This needs to be released after use.
    buf.retain()
    SparkEnv.get.blockManager.cacheSingle(
      BlockId(blockId), buf, StorageLevel.MEMORY_ONLY, tellMaster = false)
    context.taskMetrics.incDiskNanos(diskReadNanos)
    if (outstandingBlockIdToSize.isEmpty) {
      logInfo(s"Notifying LocalDagScheduler of completion of monotask $taskId")
      localDagScheduler.post(TaskSuccess(this))
    }
  }

  override def onFailure(failedBlockId: String, e: Throwable): Unit = {
    removeOutstandingBlock(failedBlockId, "onFailure")

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

  /**
   * NetworkRequestMonotask overrides cleanupIntermediateData because it needs to cleanup all
   * of the shuffle blocks fetched over the network.
   */
  override def cleanupIntermediateData(): Unit = {
    super.cleanupIntermediateData()
    val blockManager = SparkEnv.get.blockManager
    // Delete all of the shuffle blocks that were fetched over the network.
    shuffleBlockIdsAndSizes.foreach {
      case (blockId, size) =>
        blockManager.removeBlockFromMemory(blockId, tellMaster = false)
    }
  }
}

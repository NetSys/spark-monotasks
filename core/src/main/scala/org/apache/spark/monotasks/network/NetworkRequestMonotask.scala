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
import org.apache.spark.monotasks.{LocalDagSchedulerEvent, TaskSuccess}
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
 * @param lowPriority True if the Monotask should only be run opportunistically, when other
 *                    monotasks aren't running.
 */
private[spark] class NetworkRequestMonotask(
    context: TaskContextImpl,
    private val remoteAddress: BlockManagerId,
    private val shuffleBlockIdsAndSizes: Seq[(ShuffleBlockId, Long)],
    lowPriority: Boolean = false)
  extends NetworkMonotask(context) with Logging with BlockReceivedCallback
  with Comparable[NetworkRequestMonotask] {

  /** Scheduler to notify about bytes received over the network. Set by execute(). */
  private var networkScheduler: Option[NetworkScheduler] = None

  /** Time when the request for remote data was issued. */
  private var requestIssueTimeNanos = 0L

  val outstandingBlockIdToSize = new HashMap[BlockId, Long]

  override def isLowPriority(): Boolean = lowPriority

  /**
   * ID of the reducer that this monotask corresponds to. This code assumes all of the blocks that
   * will be fetched by this monotask are for the same reduce task.
   */
  val reduceId: Int = shuffleBlockIdsAndSizes(0)._1.reduceId

  override def compareTo(other: NetworkRequestMonotask): Int = {
    return this.reduceId - other.reduceId
  }

  override def toString(): String = {
    s"Monotask ${this.taskId} (${this.getClass().getName()} for macrotask " +
      s"${context.taskAttemptId} reduce ID $reduceId)}"
  }

  override def execute(scheduler: NetworkScheduler): Unit = {
    // Filter out blocks that are already available locally. This is necessary because when
    // ShuffleHelper creates the NetworkRequestMonotasks to fetch shuffle data, it only looks at the
    // MapStatuses sent by the map tasks, and doesn't check to see if any shuffle blocks have
    // already been opportunistically fetched.
    val filteredShuffleBlockIdsAndSizes = shuffleBlockIdsAndSizes.filter { blockIdAndSize =>
      !SparkEnv.get.blockManager.memoryStore.contains(blockIdAndSize._1)
    }
    if (filteredShuffleBlockIdsAndSizes.isEmpty) {
      // Mark the monotask as finished.
      logInfo(s"Notifying LocalDagScheduler of completion of monotask $taskId before it fetched " +
        s"any data, because all of its data is already present locally")
      localDagScheduler.post(TaskSuccess(this))

      // Also tell the network scheduler that this task has finished, so it knows it can launch
      // more.
      scheduler.handleNetworkRequestSatisfied(this)
    } else {
      // Populate outstandingBlockIdToSize based on which blocks need to be fetched.
      filteredShuffleBlockIdsAndSizes.foreach {
        case (shuffleBlockId, size) =>
          outstandingBlockIdToSize.put(shuffleBlockId, size)
      }
      // Issue requests to fetch all of the data.
      val blockIds = filteredShuffleBlockIdsAndSizes.map(_._1)
      val blockIdsAsString = blockIds.mkString(", ")
      val totalSize = filteredShuffleBlockIdsAndSizes.map(_._2).sum
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
          virtualSize,
          context.taskAttemptId,
          context.attemptNumber,
          this)
      } catch {
        case NonFatal(t) => {
          logError(s"Failed to initiate fetchBlock for shuffle blocks $blockIdsAsString", t)
          networkScheduler.get.handleNetworkRequestSatisfied(this)
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

    // Update the metrics about shuffle data fetched.
    // TODO: This code creates a new read metrics object for each shuffle block. Change this to be
    //       more efficient.
    val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
    readMetrics.incRemoteBlocksFetched(1)
    readMetrics.incRemoteBytesRead(buf.size())

    if (outstandingBlockIdToSize.isEmpty) {
      setFinishTime()
      logInfo(s"Notifying NetworkScheduler of completion of monotask $taskId")
      networkScheduler.get.handleNetworkRequestSatisfied(this)
      logInfo(s"Notifying LocalDagScheduler of completion of monotask $taskId")
      localDagScheduler.post(TaskSuccess(this))
    } else {
      logDebug(s"On success, monotask $this: blocks still outstanding: " +
        s"${outstandingBlockIdToSize.mkString(", ")}")
    }
  }

  override def onFailure(failedBlockId: String, e: Throwable): Unit = {
    removeOutstandingBlock(failedBlockId, "onFailure")

    if (outstandingBlockIdToSize.isEmpty) {
      logInfo(s"Notifying NetworkScheduler of completion of monotask $taskId (after failure)")
      networkScheduler.get.handleNetworkRequestSatisfied(this)
    } else {
      logInfo(s"On failure, monotask $this: blocks still outstanding: " +
        s"${outstandingBlockIdToSize.mkString(", ")}")
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

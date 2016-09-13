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

package org.apache.spark.network

import java.io.Closeable
import java.nio.ByteBuffer

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration

import org.apache.spark.Logging
import org.apache.spark.network.buffer.{NioManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.BlockReceivedCallback
import org.apache.spark.storage.BlockManager

private[spark]
abstract class BlockTransferService extends Closeable with Logging {

  /**
   * Initialize the transfer service by giving it the BlockManager that can be used to fetch
   * local blocks or put local blocks.
   */
  def init(blockManager: BlockManager)

  /**
   * Tear down the transfer service.
   */
  def close(): Unit

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  def port: Int

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  def hostName: String

  /**
   * Signals to the remote side that blocks are available on this machine to fetch. Available only
   * after [[init]] is invoked.
   */
  def signalBlocksAvailable(
      remoteHost: String,
      remotePort: Int,
      blockIds: Array[String],
      blockSizes: Array[Int],
      taskAttemptId: Long,
      attemptNumber: Int,
      localExecutorId: String,
      localHost: String,
      localBlockManagerPort: Int);

  /**
   * Fetches blocks from a remote node asynchronously, available only after [[init]] is invoked.
   */
  def fetchBlocks(
      host: String,
      port: Int,
      blockIds: Array[String],
      totalVirtualSize: Double,
      taskAttemptId: Long,
      attemptNumber: Int,
      blockReceivedCallback: BlockReceivedCallback): Unit

  /**
   * A special case of [[fetchBlocks]] that is blocking and that only fetches a single block.
   *
   * It is also only available after [[init]] is invoked.
   *
   * TODO: This is only used by TorrentBroadcast and the TaskResultGetter (when a task result is too
   *       large to be sent directly). Those uses should be converted to use network monotasks and
   *       this method should be removed.
   */
  def fetchBlockSync(host: String, port: Int, blockId: String): ManagedBuffer = {
    // A monitor for the thread to wait on.
    val result = Promise[ManagedBuffer]()
    fetchBlocks(host, port, Array(blockId), 1, -1L, -1,
      new BlockReceivedCallback {
        override def onFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        override def onSuccess(
            blockId: String,
            diskReadNanos: Long,
            totalRemoteNanos: Long,
            data: ManagedBuffer): Unit = {
          val ret = ByteBuffer.allocate(data.size.toInt)
          ret.put(data.nioByteBuffer())
          ret.flip()
          result.success(new NioManagedBuffer(ret))
        }
        override def isLowPriority(): Boolean = false
      })

    Await.result(result.future, Duration.Inf)
  }
}

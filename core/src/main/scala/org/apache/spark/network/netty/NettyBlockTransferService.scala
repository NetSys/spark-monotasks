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

package org.apache.spark.network.netty

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network.{BlockTransferService, TransportContext}
import org.apache.spark.network.client.{BlockReceivedCallback, TransportClientFactory}
import org.apache.spark.network.server.TransportServer
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at at time.
 */
class NettyBlockTransferService(conf: SparkConf, securityManager: SecurityManager, numCores: Int)
  extends BlockTransferService {

  private val transportConf = SparkTransportConf.fromSparkConf(conf, numCores)

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _

  override def init(blockManager: BlockManager): Unit = {
    transportContext = new TransportContext(transportConf, blockManager)
    clientFactory = transportContext.createClientFactory()
    server = transportContext.createServer(conf.getInt("spark.blockManager.port", 0))
    logInfo("Server created on " + server.getPort)
  }

  def signalBlocksAvailable(
      remoteHost: String,
      remotePort: Int,
      blockIds: Array[String],
      blockSizes: Array[Int],
      taskAttemptId: Long,
      attemptNumber: Int,
      localExecutorId: String,
      localHost: String,
      localBlockManagerPort: Int) {
    val client = clientFactory.createClient(remoteHost, remotePort)
    client.signalBlocksAvailable(
      blockIds,
      blockSizes,
      taskAttemptId,
      attemptNumber,
      localExecutorId,
      localHost,
      localBlockManagerPort);
  }
  override def fetchBlocks(
      host: String,
      port: Int,
      blockIds: Array[String],
      totalVirtualSize: Double,
      taskAttemptId: Long,
      attemptNumber: Int,
      blockReceivedCallback: BlockReceivedCallback): Unit = {
    logTrace(s"Fetching blocks ${blockIds.mkString(", ")} from $host:$port")
    val client = clientFactory.createClient(host, port)
    client.fetchBlocks(
      blockIds, totalVirtualSize, taskAttemptId, attemptNumber, blockReceivedCallback)
  }

  override def hostName: String = Utils.localHostName()

  override def port: Int = server.getPort

  override def close(): Unit = {
    server.close()
    clientFactory.close()
  }
}

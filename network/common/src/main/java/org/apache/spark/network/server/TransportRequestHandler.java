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

package org.apache.spark.network.server;

import java.util.Arrays;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.BlockFetchRequest;
import org.apache.spark.network.protocol.BlocksAvailable;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.util.NettyUtils;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler is
 * attached to a single Netty channel, and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link TransportServer}.
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
  private final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

  /** The Netty channel that this handler is associated with. */
  private final Channel channel;

  /** Handles returning data for a given block id. */
  private final BlockFetcher blockFetcher;

  public TransportRequestHandler(
      Channel channel,
      BlockFetcher blockFetcher) {
    this.channel = channel;
    this.blockFetcher = blockFetcher;
  }

  @Override
  public void exceptionCaught(Throwable cause) {
  }

  @Override
  public void channelUnregistered() {
    // Do nothing.
  }

  @Override
  public void handle(RequestMessage request) {
    if (request instanceof BlockFetchRequest) {
      processFetchRequest((BlockFetchRequest) request);
    } else if (request instanceof BlocksAvailable) {
      processBlocksAvailable((BlocksAvailable) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  private void processFetchRequest(final BlockFetchRequest req) {
    final String client = NettyUtils.getRemoteAddress(channel);

    logger.debug("Received req from {} to fetch blocks {}", client, Arrays.toString(req.blockIds));

    blockFetcher.getBlockData(
        req.blockIds, req.totalVirtualSize, client, channel, req.taskAttemptId, req.attemptNumber);
  }

  private void processBlocksAvailable(final BlocksAvailable blocksAvailable) {
    // Need to get the remote name here so that, when the TaskContextImpl is created, it has a
    // consistent remote name with ones used above.
    final String remoteName = NettyUtils.getRemoteAddress(channel);
    blockFetcher.signalBlocksAvailable(
        remoteName,
        blocksAvailable.blockIds,
        blocksAvailable.blockSizes,
        blocksAvailable.taskAttemptId,
        blocksAvailable.attemptNumber,
        blocksAvailable.executorId,
        blocksAvailable.host,
        blocksAvailable.blockManagerPort);
  }
}

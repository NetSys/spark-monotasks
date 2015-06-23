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

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.protocol.BlockFetchFailure;
import org.apache.spark.network.protocol.BlockFetchRequest;
import org.apache.spark.network.protocol.BlockFetchSuccess;
import org.apache.spark.network.protocol.Encodable;
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
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  private void processFetchRequest(final BlockFetchRequest req) {
    final String client = NettyUtils.getRemoteAddress(channel);

    logger.trace("Received req from {} to fetch block {}", client, req.blockId);

    ManagedBuffer buf;
    try {
      buf = blockFetcher.getBlockData(req.blockId);
    } catch (Exception e) {
      logger.error(String.format(
        "Error opening block %s for request from %s", req.blockId, client), e);
      respond(new BlockFetchFailure(req.blockId, Throwables.getStackTraceAsString(e)));
      return;
    }

    respond(new BlockFetchSuccess(req.blockId, buf));
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  private void respond(final Encodable result) {
    final String remoteAddress = channel.remoteAddress().toString();
    channel.writeAndFlush(result).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            logger.trace(String.format("Sent result %s to client %s", result, remoteAddress));
          } else {
            logger.error(String.format("Error sending result %s to %s; closing connection",
              result, remoteAddress), future.cause());
            channel.close();
          }
        }
      }
    );
  }
}

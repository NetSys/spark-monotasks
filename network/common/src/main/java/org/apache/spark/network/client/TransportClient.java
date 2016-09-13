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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.BlockFetchRequest;
import org.apache.spark.network.protocol.BlocksAvailable;
import org.apache.spark.network.util.NettyUtils;

/**
 * Client for fetching blocks from a remote host. This API is intended to allow
 * efficient transfer of large blocks of data (block size ranges from hundreds of KB to a few MB).
 *
 * Construct an instance of TransportClient using {@link TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportClient implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;
  private final TransportResponseHandler handler;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
  }

  public boolean isActive() {
    return channel.isOpen() || channel.isActive();
  }

  /**
   * Signals to the remote side that blocks are available on this machine to fetch.
   */
  public void signalBlocksAvailable(
      String[] blockIds,
      int[] blockSizes,
      long taskAttemptId,
      int attemptNumber,
      String executorId,
      String host,
      int port) {
    BlocksAvailable message = new BlocksAvailable(
      blockIds, blockSizes, taskAttemptId, attemptNumber, executorId, host, port);
    channel.writeAndFlush(message);
  }

  /**
   * Requests blocks from the remote side.
   *
   * Multiple fetchBlocks requests may be outstanding simultaneously. If the blocks reside in-memory
   * on the remote host, they will be returned in the same order that they were requested
   * (assuming that only one client is used); otherwise, requests may be re-ordered if some requests
   * need to be read from a disk with a longer queue.
   *
   * @param rawBlockIds Identifiers of the blocks that should be fetched.
   * @param callback Callback invoked upon successful receipt of each block, or upon any failure.
   */
  public void fetchBlocks(
      final String[] rawBlockIds,
      double totalVirtualSize,
      Long taskAttemptId,
      int attemptNumber,
      final BlockReceivedCallback callback) {
    ArrayList<String> blockIdsToFetch = new ArrayList<String>();
    for (String blockId : rawBlockIds) {
      // If addFetchRequest returns false, it means there was already an outstanding request for
      // that block, so don't add it to the blocks we're requesting.
      if (handler.addFetchRequest(blockId, callback)) {
        blockIdsToFetch.add(blockId);
      }
    }
    final String[] blockIds = new String[blockIdsToFetch.size()];
    for (int i = 0; i < blockIds.length; i++) {
        blockIds[i] = blockIdsToFetch.get(i);
    }

    final String serverAddr = NettyUtils.getRemoteAddress(channel);
    final String blockIdsAsStrings = Arrays.toString(blockIds);
    final long startTime = System.currentTimeMillis();
    logger.debug("Sending request for blocks {} to {}", blockIdsAsStrings, serverAddr);

    BlockFetchRequest request =
        new BlockFetchRequest(blockIds, totalVirtualSize, taskAttemptId, attemptNumber);
    channel.writeAndFlush(request).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            long timeTaken = System.currentTimeMillis() - startTime;
            logger.trace("Sending request for {} to {} took {} ms", blockIdsAsStrings, serverAddr,
              timeTaken);
          } else {
            String errorMsg = String.format("Failed to send request for %s to %s: %s",
              blockIdsAsStrings, serverAddr, future.cause());
            logger.error(errorMsg, future.cause());
            channel.close();
            for (String blockId: blockIds) {
              handler.removeFetchRequest(blockId);
              try {
                callback.onFailure(blockId, new IOException(errorMsg, future.cause()));
              } catch (Exception e) {
                logger.error("Uncaught exception in RPC response callback handler!", e);
              }
            }
          }
        }
      });
  }

  @Override
  public void close() {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("remoteAdress", channel.remoteAddress())
      .add("isActive", isActive())
      .toString();
  }
}

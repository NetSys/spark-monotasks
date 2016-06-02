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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.BlockFetchFailure;
import org.apache.spark.network.protocol.BlockFetchSuccess;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.server.MessageHandler;
import org.apache.spark.network.util.NettyUtils;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
  private final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private final Channel channel;

  private final Map<String, BlockReceivedCallback> outstandingFetches;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingFetches = new ConcurrentHashMap<String, BlockReceivedCallback>();
  }

  /**
   * Attempts to add an outstanding fetch request.  If another request is already outstanding for
   * the given block, returns false and keeps the callback corresponding to the higher priority
   * request (this assumes that at most one high priority request will be outstanding concurrently).
   * Otherwise, returns true.
   */
  public synchronized boolean addFetchRequest(String blockId, BlockReceivedCallback callback) {
    if (outstandingFetches.containsKey(blockId)) {
      BlockReceivedCallback existingCallback = outstandingFetches.get(blockId);
      if (existingCallback.isLowPriority()) {
        // Replace the existing callback with the new one, and fail the existing callback.
        existingCallback.onFailure(blockId, new BlockFetchFailureException(
            "Low-priority request to fetch " + blockId + " failed so high priority can take over"));
      } else {
        // The new request is low-priority, so shouldn't be executed. Fail the new one.
        callback.onFailure(blockId, new BlockFetchFailureException(
            "Low-priority request to fetch " + blockId +
            "failed because high priority one already running"));
        return false;
      }
    }
    outstandingFetches.put(blockId, callback);
    return true;
  }

  public void removeFetchRequest(String blockId) {
    outstandingFetches.remove(blockId);
  }

  /**
   * Fire the failure callback for all outstanding requests. This is called when we have an
   * uncaught exception or pre-mature connection termination.
   */
  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<String, BlockReceivedCallback> entry : outstandingFetches.entrySet()) {
      entry.getValue().onFailure(entry.getKey(), cause);
    }

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingFetches.clear();
  }

  @Override
  public void channelUnregistered() {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  @Override
  public synchronized void handle(ResponseMessage message) {
    String remoteAddress = NettyUtils.getRemoteAddress(channel);
    if (message instanceof BlockFetchSuccess) {
      BlockFetchSuccess resp = (BlockFetchSuccess) message;
      BlockReceivedCallback listener = outstandingFetches.get(resp.blockId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} since it is not outstanding",
          resp.blockId, remoteAddress);
        resp.buffer.release();
      } else {
        outstandingFetches.remove(resp.blockId);
        listener.onSuccess(resp.blockId, resp.diskReadNanos, resp.totalRemoteNanos, resp.buffer);
        resp.buffer.release();
      }
    } else if (message instanceof BlockFetchFailure) {
      BlockFetchFailure resp = (BlockFetchFailure) message;
      BlockReceivedCallback listener = outstandingFetches.get(resp.blockId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
          resp.blockId, remoteAddress, resp.errorString);
      } else {
        outstandingFetches.remove(resp.blockId);
        listener.onFailure(resp.blockId, new BlockFetchFailureException(
          "Failure while fetching " + resp.blockId + ": " + resp.errorString));
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  /** Returns total number of outstanding block fetch requests. */
  @VisibleForTesting
  public int numOutstandingRequests() {
    return outstandingFetches.size();
  }
}

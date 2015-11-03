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

package org.apache.spark.network;

import io.netty.channel.local.LocalChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.BlockReceivedCallback;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.BlockFetchFailure;
import org.apache.spark.network.protocol.BlockFetchSuccess;

public class TransportResponseHandlerSuite {
  @Test
  public void handleSuccessfulFetch() {
    String blockId = "rdd_1_0";

    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    BlockReceivedCallback callback = mock(BlockReceivedCallback.class);
    handler.addFetchRequest(blockId, callback);
    assertEquals(1, handler.numOutstandingRequests());

    Long diskReadNanos = 14L;
    Long totalRemoteNanos = 120L;
    handler.handle(
      new BlockFetchSuccess(blockId, new TestManagedBuffer(123), diskReadNanos, totalRemoteNanos));
    verify(callback).onSuccess(
      eq(blockId), eq(diskReadNanos), eq(totalRemoteNanos), (ManagedBuffer) any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleFailedFetch() {
    String blockId = "rdd_1_0";
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    BlockReceivedCallback callback = mock(BlockReceivedCallback.class);
    handler.addFetchRequest(blockId, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new BlockFetchFailure(blockId, "some error msg"));
    verify(callback).onFailure(eq(blockId), (Throwable) any());
    assertEquals(0, handler.numOutstandingRequests());
  }
}

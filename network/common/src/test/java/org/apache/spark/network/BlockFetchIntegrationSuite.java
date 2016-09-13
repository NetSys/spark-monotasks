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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.BlockReceivedCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.protocol.BlockFetchFailure;
import org.apache.spark.network.protocol.BlockFetchSuccess;
import org.apache.spark.network.server.BlockFetcher;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class BlockFetchIntegrationSuite {
  static final String BUFFER_BLOCK_ID = "buffer";
  static final String FILE_BLOCK_ID = "file";

  static TransportServer server;
  static TransportClientFactory clientFactory;
  static File testFile;

  static ManagedBuffer bufferChunk;
  static ManagedBuffer fileChunk;

  @BeforeClass
  public static void setUp() throws Exception {
    int bufSize = 100000;
    final ByteBuffer buf = ByteBuffer.allocate(bufSize);
    for (int i = 0; i < bufSize; i ++) {
      buf.put((byte) i);
    }
    buf.flip();
    bufferChunk = new NioManagedBuffer(buf);

    testFile = File.createTempFile("shuffle-test-file", "txt");
    testFile.deleteOnExit();
    RandomAccessFile fp = new RandomAccessFile(testFile, "rw");
    byte[] fileContent = new byte[1024];
    new Random().nextBytes(fileContent);
    fp.write(fileContent);
    fp.close();

    final TransportConf conf = new TransportConf(new SystemPropertyConfigProvider());
    fileChunk = new FileSegmentManagedBuffer(conf, testFile, 10, testFile.length() - 25);

    BlockFetcher blockFetcher = new BlockFetcher() {
      @Override
      public void getBlockData(
          String[] blockIds, double totalVirtualSize, String remoteName, Channel channel,
          long taskAttemptId, int attemptNumber) {
        for (String blockId : blockIds) {
          if (blockId.equals(BUFFER_BLOCK_ID)) {
            channel.writeAndFlush(
              new BlockFetchSuccess(blockId, new NioManagedBuffer(buf), 0L, 0L));
          } else if (blockId.equals(FILE_BLOCK_ID)) {
            ManagedBuffer buffer =
              new FileSegmentManagedBuffer(conf, testFile, 10, testFile.length() - 25);
            channel.writeAndFlush(new BlockFetchSuccess(blockId, buffer, 0L, 0L));
          } else {
            channel.writeAndFlush(
              new BlockFetchFailure(blockId, "Invalid chunk index: " + blockId));
          }
        }
      }

      @Override
      public void signalBlocksAvailable(
          String remoteName,
          String[] blockIds,
          int[] blockSizes,
          long taskAttemptId,
          int attemptNumber,
          String executorId,
          String host,
          int blockManagerPort) {
          // Do nothing.
      }
    };

    TransportContext context = new TransportContext(conf, blockFetcher);
    server = context.createServer();
    clientFactory = context.createClientFactory();
  }

  @AfterClass
  public static void tearDown() {
    server.close();
    clientFactory.close();
    testFile.delete();
  }

  class FetchResult {
    public Set<String> successBlocks;
    public Set<String> failedBlocks;
    public List<ManagedBuffer> buffers;

    public void releaseBuffers() {
      for (ManagedBuffer buffer : buffers) {
        buffer.release();
      }
    }
  }

  private FetchResult fetchBlock(String blockId) throws Exception {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    final Semaphore sem = new Semaphore(0);

    final FetchResult res = new FetchResult();
    res.successBlocks = Collections.synchronizedSet(new HashSet<String>());
    res.failedBlocks = Collections.synchronizedSet(new HashSet<String>());
    res.buffers = Collections.synchronizedList(new LinkedList<ManagedBuffer>());

    BlockReceivedCallback callback = new BlockReceivedCallback() {
      @Override
      public void onSuccess(
          String blockId, long diskReadNanos, long totalRemoteNanos, ManagedBuffer buffer) {
        buffer.retain();
        res.successBlocks.add(blockId);
        res.buffers.add(buffer);
        sem.release();
      }

      @Override
      public void onFailure(String blockId, Throwable e) {
        res.failedBlocks.add(blockId);
        sem.release();
      }

      @Override
      public boolean isLowPriority() {
        return false;
      }
    };

    client.fetchBlocks(new String[]{blockId}, 1, 0L, 0, callback);
    if (!sem.tryAcquire(1, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    client.close();
    return res;
  }

  @Test
  public void fetchBufferChunk() throws Exception {
    FetchResult res = fetchBlock(BUFFER_BLOCK_ID);
    assertEquals(res.successBlocks, Sets.newHashSet(BUFFER_BLOCK_ID));
    assertTrue(res.failedBlocks.isEmpty());
    assertBufferListsEqual(res.buffers, Lists.newArrayList(bufferChunk));
    res.releaseBuffers();
  }

  @Test
  public void fetchFileChunk() throws Exception {
    FetchResult res = fetchBlock(FILE_BLOCK_ID);
    assertEquals(res.successBlocks, Sets.newHashSet(FILE_BLOCK_ID));
    assertTrue(res.failedBlocks.isEmpty());
    assertBufferListsEqual(res.buffers, Lists.newArrayList(fileChunk));
    res.releaseBuffers();
  }

  @Test
  public void fetchNonExistentChunk() throws Exception {
    FetchResult res = fetchBlock("12345");
    assertTrue(res.successBlocks.isEmpty());
    assertEquals(res.failedBlocks, Sets.newHashSet("12345"));
    assertTrue(res.buffers.isEmpty());
  }

  private void assertBufferListsEqual(List<ManagedBuffer> list0, List<ManagedBuffer> list1)
      throws Exception {
    assertEquals(list0.size(), list1.size());
    for (int i = 0; i < list0.size(); i ++) {
      assertBuffersEqual(list0.get(i), list1.get(i));
    }
  }

  private void assertBuffersEqual(ManagedBuffer buffer0, ManagedBuffer buffer1) throws Exception {
    ByteBuffer nio0 = buffer0.nioByteBuffer();
    ByteBuffer nio1 = buffer1.nioByteBuffer();

    int len = nio0.remaining();
    assertEquals(nio0.remaining(), nio1.remaining());
    for (int i = 0; i < len; i ++) {
      assertEquals(nio0.get(), nio1.get());
    }
  }
}

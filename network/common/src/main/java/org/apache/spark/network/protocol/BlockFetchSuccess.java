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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

/**
 * Response to {@link BlockFetchRequest} when a block exists and has been successfully fetched.
 *
 * Note that the server-side encoding of this messages does NOT include the buffer itself, as this
 * may be written by Netty in a more efficient manner (i.e., zero-copy write).
 * Similarly, the client-side decoding will reuse the Netty ByteBuf as the buffer.
 */
public final class BlockFetchSuccess implements ResponseMessage {
  public final String blockId;
  public final ManagedBuffer buffer;
  public final Long diskReadNanos;
  /**
   * Time elapsed on the remote machine, from when the machine received the request for a remote
   * block until the machine sent a response.
   */
  public final Long totalRemoteNanos;

  public BlockFetchSuccess(
      String blockId,
      ManagedBuffer buffer,
      Long diskReadNanos,
      Long totalRemoteNanos) {
    this.blockId = blockId;
    this.buffer = buffer;
    this.diskReadNanos = diskReadNanos;
    this.totalRemoteNanos = totalRemoteNanos;
  }

  @Override
  public Type type() { return Type.BlockFetchSuccess; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(blockId) + 8 + 8;
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, blockId);
    buf.writeLong(diskReadNanos);
    buf.writeLong(totalRemoteNanos);
  }

  /** Decoding uses the given ByteBuf as our data, and will retain() it. */
  public static BlockFetchSuccess decode(ByteBuf buf) {
    String blockId = Encoders.Strings.decode(buf);
    Long diskReadNanos = buf.readLong();
    Long totalFetchTimeNanos = buf.readLong();
    buf.retain();
    NettyManagedBuffer managedBuf = new NettyManagedBuffer(buf.duplicate());
    return new BlockFetchSuccess(blockId, managedBuf, diskReadNanos, totalFetchTimeNanos);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BlockFetchSuccess) {
      BlockFetchSuccess o = (BlockFetchSuccess) other;
      return (blockId.equals(o.blockId) && buffer.equals(o.buffer) &&
        (diskReadNanos == o.diskReadNanos) && (totalRemoteNanos == o.totalRemoteNanos));
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("blockId", blockId)
      .add("buffer", buffer)
      .add("diskReadNanos", diskReadNanos)
      .add("totalRemoteNanos", totalRemoteNanos)
      .toString();
  }
}

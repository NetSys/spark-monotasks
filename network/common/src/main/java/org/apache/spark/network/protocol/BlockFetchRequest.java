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

import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * Request to fetch a remote block. This will correspond to a single
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 */
public final class BlockFetchRequest implements RequestMessage {
  public final String[] blockIds;
  public final double totalVirtualSize;
  public final Long taskAttemptId;
  public final int attemptNumber;

  public BlockFetchRequest(
      String[] blockIds, double totalVirtualSize, Long taskAttemptId, int attemptNumber) {
    this.blockIds = blockIds;
    this.totalVirtualSize = totalVirtualSize;
    this.taskAttemptId = taskAttemptId;
    this.attemptNumber = attemptNumber;
  }

  @Override
  public Type type() {
    return Type.BlockFetchRequest;
  }

  @Override
  public int encodedLength() {
    return Encoders.StringArrays.encodedLength(blockIds) + 8 + 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.StringArrays.encode(buf, blockIds);
    buf.writeDouble(totalVirtualSize);
    buf.writeLong(taskAttemptId);
    buf.writeInt(attemptNumber);
  }

  public static BlockFetchRequest decode(ByteBuf buf) {
    String[] blockIds = Encoders.StringArrays.decode(buf);
    double totalVirtualSize = buf.readDouble();
    Long taskAttemptId = buf.readLong();
    int attemptNumber = buf.readInt();
    return new BlockFetchRequest(blockIds, totalVirtualSize, taskAttemptId, attemptNumber);

  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BlockFetchRequest) {
      BlockFetchRequest o = (BlockFetchRequest) other;
      return ((Arrays.equals(blockIds, o.blockIds)) && (totalVirtualSize == o.totalVirtualSize) &&
        (taskAttemptId == o.taskAttemptId) && (attemptNumber == o.attemptNumber));
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("blockIds", Arrays.toString(blockIds))
      .add("totalVirtualSize", totalVirtualSize)
      .add("taskAttemptId", taskAttemptId)
      .add("attemptNumber", attemptNumber)
      .toString();
  }
}

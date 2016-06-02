/*
 * Copyright 2016 The Regents of The University California
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

import java.util.Arrays;

/**
 * Notification that blocks are available to be fetched from a machine.
 *
 * A BlocksAvailable message should trigger a BlockFetchRequest message as soon as the receiving
 * machine has enough bandwidth to begin the network transfer.
 */
public final class BlocksAvailable implements RequestMessage {
  /** Blocks that are available to be fetched. */
  public final String[] blockIds;

  /**
   * Sizes (in bytes) of the blocks that are available. The size at a particular index in this
   * array should correspond to the block at the same index in blockIds.
   */
  public final int[] blockSizes;

  public final Long taskAttemptId;
  public final int attemptNumber;

  /** Executor on which the blocks are available. */
  public final String executorId;
  public final String host;
  public final int blockManagerPort;

  public BlocksAvailable(
      String[] blockIds,
      int[] blockSizes,
      Long taskAttemptId,
      int attemptNumber,
      String executorId,
      String host,
      int blockManagerPort) {
    this.blockIds = blockIds;
    this.blockSizes = blockSizes;
    this.taskAttemptId = taskAttemptId;
    this.attemptNumber = attemptNumber;
    this.executorId = executorId;
    this.host = host;
    this.blockManagerPort = blockManagerPort;
  }

  @Override
  public Type type() {
    return Type.BlocksAvailable;
  }

  @Override
  public int encodedLength() {
    return (Encoders.StringArrays.encodedLength(blockIds) +
      Encoders.IntArrays.encodedLength(blockSizes) +
      8 + // taskAttemptId
      4 + // attemptNumber
      Encoders.Strings.encodedLength(executorId) +
      Encoders.Strings.encodedLength(host) +
      4 // blockManagerPort
    );
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.StringArrays.encode(buf, blockIds);
    Encoders.IntArrays.encode(buf, blockSizes);
    buf.writeLong(taskAttemptId);
    buf.writeInt(attemptNumber);
    Encoders.Strings.encode(buf, executorId);
    Encoders.Strings.encode(buf, host);
    buf.writeInt(blockManagerPort);
  }

  public static BlocksAvailable decode(ByteBuf buf) {
    String[] blockIds = Encoders.StringArrays.decode(buf);
    int[] blockSizes = Encoders.IntArrays.decode(buf);
    Long taskAttemptId = buf.readLong();
    int attemptNumber = buf.readInt();
    String executorId = Encoders.Strings.decode(buf);
    String host = Encoders.Strings.decode(buf);
    int blockManagerPort = buf.readInt();
    return new BlocksAvailable(
      blockIds, blockSizes, taskAttemptId, attemptNumber, executorId, host, blockManagerPort);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BlocksAvailable) {
      BlocksAvailable o = (BlocksAvailable) other;
      return ((Arrays.equals(blockIds, o.blockIds)) && Arrays.equals(blockSizes, o.blockSizes) &&
        (taskAttemptId == o.taskAttemptId) && (attemptNumber == o.attemptNumber) &&
        executorId.equals(o.executorId) && host.equals(o.host) &&
        (blockManagerPort == o.blockManagerPort));
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("blockIds", Arrays.toString(blockIds))
      .add("blockSizes", Arrays.toString(blockSizes))
      .add("taskAttemptId", taskAttemptId)
      .add("attemptNumber", attemptNumber)
      .add("executorId", executorId)
      .add("host", host)
      .add("blockManagerPort", blockManagerPort)
      .toString();
  }
}

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

import io.netty.channel.Channel;

/**
 * Interface for asynchronously getting data identified by a set of block identifiers. The requested
 * data (or an error, if applicable) is sent back on the given channel.
 *
 * This interface is also used to signal that a set of blocks are available on a remote machine.
 *
 * Used by TransportRequestHandler to service requests from remote hosts for particular blocks.
 */
public abstract class BlockFetcher {
  public abstract void getBlockData(
    String[] blockIds, double totalVirtualSize, String remoteName, Channel channel,
    long taskAttemptId, int attemptNumber);

  public abstract void signalBlocksAvailable(
    String remoteName,
    String[] blockIds,
    int[] blockSizes,
    long taskAttemptId,
    int attemptNumber,
    String executorId,
    String host,
    int blockManagerPort);
}

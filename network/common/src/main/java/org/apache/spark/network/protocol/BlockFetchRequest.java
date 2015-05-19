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

/**
 * Request to fetch a remote block. This will correspond to a single
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 */
public final class BlockFetchRequest implements RequestMessage {
  public final String blockId;

  public BlockFetchRequest(String blockId) {
    this.blockId = blockId;
  }

  @Override
  public Type type() {
    return Type.BlockFetchRequest;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(blockId);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, blockId);
  }

  public static BlockFetchRequest decode(ByteBuf buf) {
    return new BlockFetchRequest(Encoders.Strings.decode(buf));
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BlockFetchRequest) {
      BlockFetchRequest o = (BlockFetchRequest) other;
      return blockId.equals(o.blockId);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("blockId", blockId)
      .toString();
  }
}

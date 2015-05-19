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
 * Response to {@link BlockFetchRequest} when there is an error fetching the chunk.
 */
public final class BlockFetchFailure implements ResponseMessage {
  public final String blockId;
  public final String errorString;

  public BlockFetchFailure(String blockId, String errorString) {
    this.blockId = blockId;
    this.errorString = errorString;
  }

  @Override
  public Type type() { return Type.BlockFetchFailure; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(blockId) + Encoders.Strings.encodedLength(errorString);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, blockId);
    Encoders.Strings.encode(buf, errorString);
  }

  public static BlockFetchFailure decode(ByteBuf buf) {
    String blockId = Encoders.Strings.decode(buf);
    String errorString = Encoders.Strings.decode(buf);
    return new BlockFetchFailure(blockId, errorString);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BlockFetchFailure) {
      BlockFetchFailure o = (BlockFetchFailure) other;
      return blockId.equals(o.blockId) && errorString.equals(o.errorString);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("blockId", blockId)
      .add("errorString", errorString)
      .toString();
  }
}

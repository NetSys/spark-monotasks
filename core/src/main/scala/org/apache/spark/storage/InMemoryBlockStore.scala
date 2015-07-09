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

package org.apache.spark.storage

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging

/** Abstract class to store blocks in memory. */
private[spark] abstract class InMemoryBlockStore(val blockManager: BlockManager) extends Logging {

  def cacheBytes(blockId: BlockId, bytes: ByteBuffer, deserialized: Boolean): CacheResult

  /**
   * Cache a block and, possibly, also return its contents as either bytes or another Iterator.
   * This is used to efficiently write the values to multiple locations (e.g. for replication).
   *
   * @return a CacheResult that contains the size of the data, as well as the values cached if
   *         returnValues is true (if not, the result's data field can be null)
   */
  def cacheIterator(
    blockId: BlockId,
    values: Iterator[Any],
    deserialized: Boolean,
    returnValues: Boolean): CacheResult

  def cacheArray(
    blockId: BlockId,
    values: Array[Any],
    deserialized: Boolean,
    returnValues: Boolean): CacheResult

  /**
   * Return the size of a block in bytes.
   */
  def getSize(blockId: BlockId): Long

  def getBytes(blockId: BlockId): Option[ByteBuffer]

  def getValues(blockId: BlockId): Option[Iterator[Any]]

  /**
   * Remove a block, if it exists.
   * @param blockId the block to remove.
   * @return True if the block was found and removed, False otherwise.
   */
  def remove(blockId: BlockId): Boolean

  def contains(blockId: BlockId): Boolean

  def clear() { }
}

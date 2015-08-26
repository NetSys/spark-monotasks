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

import scala.collection.JavaConversions._

import java.nio.ByteBuffer
import java.util.LinkedHashMap

import org.apache.spark.util.{SizeEstimator, Utils}

private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends InMemoryBlockStore(blockManager) {

  private val conf = blockManager.conf
  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)

  @volatile private var currentMemory = 0L

  // Used to ensure that only one thread is caching blocks at any given time.
  private val accountingLock = new Object

  logInfo(s"MemoryStore started with capacity ${Utils.bytesToString(maxMemory)}")

  /** Free memory not occupied by existing blocks. */
  def freeMemory: Long = maxMemory - currentMemory

  /** For testing only. Returns all of the BlockIds stored by this MemoryStore. */
  def getAllBlockIds(): Seq[BlockId] =
    entries.keys.toSeq

  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  override def cacheBytes(
      blockId: BlockId,
      bytes: ByteBuffer,
      deserialized: Boolean): CacheResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    val bytesCopy = bytes.duplicate()
    bytesCopy.rewind()
    if (deserialized) {
      val values = blockManager.dataDeserialize(blockId, bytesCopy)
      cacheIterator(blockId, values, deserialized, true)
    } else {
      tryToCache(blockId, bytesCopy, bytesCopy.limit, deserialized)
      CacheResult(bytesCopy.limit(), Right(bytesCopy.duplicate()))
    }
  }

  override def cacheArray(
      blockId: BlockId,
      values: Array[Any],
      deserialized: Boolean,
      returnValues: Boolean): CacheResult = {
    if (deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      tryToCache(blockId, values, sizeEstimate, deserialized)
      CacheResult(sizeEstimate, Left(values.iterator))
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      tryToCache(blockId, bytes, bytes.limit, deserialized)
      CacheResult(bytes.limit(), Right(bytes.duplicate()))
    }
  }

  /** Unroll the provided iterator and cache the resulting values. */
  override def cacheIterator(
      blockId: BlockId,
      values: Iterator[Any],
      deserialized: Boolean,
      returnValues: Boolean): CacheResult =
    cacheArray(blockId, values.toArray, deserialized, returnValues)

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[Array[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[Array[Any]].iterator)
    } else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }

  override def remove(blockId: BlockId): Boolean = {
    entries.synchronized {
      val entry = entries.remove(blockId)
      if (entry != null) {
        currentMemory -= entry.size
        logInfo(s"Block $blockId of size ${Utils.bytesToString(entry.size)} removed from memory " +
          s"(free: ${Utils.bytesToString(freeMemory)}).")
        true
      } else {
        false
      }
    }
  }

  override def clear() {
    entries.synchronized {
      entries.clear()
      currentMemory = 0
    }
    logInfo("MemoryStore cleared")
  }

  /**
   * Try to cache a value, if we there is enough free space. The value should either be an Array
   * if `deserialized` is true or a ByteBuffer otherwise. Its (possibly estimated) size must also be
   * passed by the caller. The caller can check if the cache operation was successful by trying to
   * get the block.
   *
   * Synchronize on `accountingLock` so that one thread does not use up free space that another
   * thread intended to use.
   *
   * TODO: If there is not enough free space to store the provided block, drop other blocks to disk.
   */
  private def tryToCache(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean) = {
    accountingLock.synchronized {
      val enoughFreeSpace = ensureFreeSpace(blockId, size)
      if (enoughFreeSpace) {
        val entry = new MemoryEntry(value, size, deserialized)
        entries.synchronized {
          entries.put(blockId, entry)
          currentMemory += size
        }
        val valuesOrBytes = if (deserialized) "values" else "bytes"
        logInfo(s"Block $blockId stored as $valuesOrBytes in memory (estimated size:  " +
          s"${Utils.bytesToString(size)}, free: ${Utils.bytesToString(freeMemory)}).")
      }
    }
  }

  /** Return whether there is enough free space to cache the specified block. */
  private def ensureFreeSpace(
      blockIdToAdd: BlockId,
      space: Long): Boolean = {
    logInfo(s"ensureFreeSpace(${Utils.bytesToString(space)}) called with " +
      s"curMem=${Utils.bytesToString(currentMemory)}, maxMem=${Utils.bytesToString(maxMemory)}")

    if (space > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd because it is larger than our memory limit.")
      return false
    }

    if (freeMemory < space) {
      logInfo(s"Will not store $blockIdToAdd because it does not fit in the available memory.")
      return false
    }
    true
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }
}

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
 *
 * @param blockManager BlockManager to use to (de)serialize data.
 * @param targetMaxMemory Maximum amount of memory intended to be used by the memory store.
 *                        The MemoryStore does not enforce this maxmimum and is only responsible
 *                        for tracking the amount of memory currently in use; calling classes are
 *                        responsible for ensuring that this number is not exceeded.
 */
private[spark] class MemoryStore(
    blockManager: BlockManager,
    private val targetMaxMemory: Long) extends InMemoryBlockStore(blockManager) {

  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)

  @volatile private var currentMemory = 0L

  /** Callback to be executed when data is removed from the memory store. */
  private var blockRemovalCallback: Option[Long => Unit] = None

  logInfo(s"MemoryStore started with capacity ${Utils.bytesToString(targetMaxMemory)}")

  /** Free memory not occupied by existing blocks. */
  def freeMemory: Long = targetMaxMemory - currentMemory

  /**
   * Registers a function to be called with the current amount of free memory anytime a block is
   * removed from the memory store.
   */
  def registerBlockRemovalCallback(callback: Long => Unit): Unit = {
    if (blockRemovalCallback.isDefined) {
      throw new IllegalStateException(
        "At most one callback to be called on block removal can be registered with the " +
        s"MemoryStore (registerBlockRemovalCallback called with $callback when " +
        s"$blockRemovalCallback was already registered")
    }
    blockRemovalCallback = Some(callback)
  }

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
      doCache(blockId, bytesCopy, bytesCopy.limit, deserialized)
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
      doCache(blockId, values, sizeEstimate, deserialized)
      CacheResult(sizeEstimate, Left(values.iterator))
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      doCache(blockId, bytes, bytes.limit, deserialized)
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
        logDebug(s"Block $blockId of size ${Utils.bytesToString(entry.size)} removed from memory " +
          s"(free: ${Utils.bytesToString(freeMemory)}).")
        blockRemovalCallback.map(_(freeMemory))
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
      blockRemovalCallback.map(_(freeMemory))
    }
    logInfo("MemoryStore cleared")
  }

  /**
   * Caches the given value. The value should either be an Array if `deserialized` is true or a
   * ByteBuffer otherwise. Its (possibly estimated) size must also be passed by the caller.
   */
  private def doCache(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean): Unit = {
    val entry = new MemoryEntry(value, size, deserialized)
    entries.synchronized {
      entries.put(blockId, entry)
      currentMemory += size

      val valuesOrBytes = if (deserialized) "values" else "bytes"
      logDebug(s"Block $blockId stored as $valuesOrBytes in memory (estimated size:  " +
        s"${Utils.bytesToString(size)}; (${Utils.bytesToString(currentMemory)} stored out of " +
        s"${Utils.bytesToString(targetMaxMemory)} target maximum)")
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }
}

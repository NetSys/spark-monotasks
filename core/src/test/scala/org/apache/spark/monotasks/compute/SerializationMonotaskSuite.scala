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

package org.apache.spark.monotasks.compute

import java.lang.IllegalStateException
import java.nio.ByteBuffer

import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.{SparkEnv, TaskContextImpl}
import org.apache.spark.storage.{BlockManager, BlockResult, TestBlockId}

class SerializationMonotaskSuite extends FunSuite with BeforeAndAfterEach {

  private var blockManager: BlockManager = _
  private val taskContext = new TaskContextImpl(0, 0)

  override def beforeEach() {
    blockManager = mock(classOf[BlockManager])
    when(blockManager.dataSerialize(any(), any(), any())).thenReturn(mock(classOf[ByteBuffer]))

    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(sparkEnv)
  }

  test("execute: returns None on success") {
    val blockId = new TestBlockId("0")
    when(blockManager.get(blockId)).thenReturn(
      Some(new BlockResult(mock(classOf[Iterator[Int]]), null, 0)))
    assert(new SerializationMonotask(taskContext, blockId).execute().isEmpty)
  }

  test("execute: throws an IllegalStateException when block to be serialized cannot be found") {
    val blockId = new TestBlockId("0")
    when(blockManager.get(blockId)).thenReturn(None)
    try {
      new SerializationMonotask(taskContext, blockId).execute()
      assert(false, "SerializationMonotask.execute() should have thrown an IllegalStateException")
    } catch {
      case e: IllegalStateException => assert(true)
      case _: Throwable => assert(false, "SerializationMonotask.execute() should have thrown an " +
        "IllegalStateException")
    }
  }
}

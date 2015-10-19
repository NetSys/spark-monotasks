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

package org.apache.spark.monotasks.disk

import java.io.File
import java.nio.ByteBuffer
import java.util.Random

import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, verify, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{SparkConf, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage._
import org.apache.spark.util.Utils

class DiskReadMonotaskSuite extends FunSuite with BeforeAndAfter {

  private var taskContext: TaskContextImpl = _
  private var blockFileManager: BlockFileManager = _
  private var blockManager: BlockManager = _
  private val dataBuffer = ByteBuffer.wrap((1 to 1000).map(_.toByte).toArray)
  private val serializedDataBlockId = new MonotaskResultBlockId(0L)

  before {
    blockFileManager = mock(classOf[BlockFileManager])

    // Pass in false to the SparkConf constructor so that the same configuration is loaded
    // regardless of the system properties.
    val testFile =
      new File(Utils.getLocalDir(new SparkConf(false)) + (new Random).nextInt(Integer.MAX_VALUE))
    testFile.deleteOnExit()
    when(blockFileManager.getBlockFile(any(), any())).thenReturn(Some(testFile))

    blockManager = mock(classOf[BlockManager])
    when(blockManager.blockFileManager).thenReturn(blockFileManager)
    when(blockManager.getStatus(any())).thenReturn(Some(mock(classOf[BlockStatus])))
    when(blockManager.getLocalBytes(serializedDataBlockId)).thenReturn(Some(dataBuffer))

    val result = Seq((mock(classOf[BlockId]), mock(classOf[BlockStatus])))
    when(blockManager.cacheBytes(any(), any(), any(), any())).thenReturn(result)

    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(sparkEnv)

    taskContext = mock(classOf[TaskContextImpl])
    when(taskContext.taskMetrics).thenReturn(TaskMetrics.empty)
  }

  test("execute: throws an exception when trying to read a nonexistent block") {
    when(blockFileManager.getBlockFile(any(), any())).thenReturn(None)
    try {
      new DiskReadMonotask(taskContext, mock(classOf[BlockId]), "nonsense").execute()
      fail(
        "This line should not have been reached because execute() should have thrown an exception.")
    } catch {
      case _: IllegalStateException => // Okay
      case _: Throwable => fail("execute() should have thrown an IllegalStateException")
    }
  }

  test("execute: correctly stores resulting data in the MemoryStore") {
    val blockId = new TestBlockId("0")
    // Write a block to verify that it can be read back correctly.
    val writeMonotask = new DiskWriteMonotask(taskContext, blockId, serializedDataBlockId)
    val diskId = "diskId"
    writeMonotask.diskId = Some(diskId)

    writeMonotask.execute()
    new DiskReadMonotask(taskContext, blockId, diskId).execute()
    verify(blockManager).cacheBytes(blockId, dataBuffer, StorageLevel.MEMORY_ONLY_SER, false)
  }
}

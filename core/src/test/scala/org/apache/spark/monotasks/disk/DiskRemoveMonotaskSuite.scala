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
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Random

import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{SparkConf, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.{BlockFileManager, BlockManager, BlockStatus, MonotaskResultBlockId,
  StorageLevel, TestBlockId}
import org.apache.spark.util.Utils

class DiskRemoveMonotaskSuite extends FunSuite with BeforeAndAfter {

  private var taskContext: TaskContextImpl = _
  private var testFile: File = _
  private val serializedDataBlockId = new MonotaskResultBlockId(0L)

  before {
    val blockFileManager = mock(classOf[BlockFileManager])

    // Pass in false to the SparkConf constructor so that the same configuration is loaded
    // regardless of the system properties.
    testFile =
      new File(Utils.getLocalDir(new SparkConf(false)) + (new Random).nextInt(Integer.MAX_VALUE))
    testFile.deleteOnExit()
    when(blockFileManager.getBlockFile(any(), any())).thenReturn(Some(testFile))

    val blockManager = mock(classOf[BlockManager])
    when(blockManager.blockFileManager).thenReturn(blockFileManager)
    when(blockManager.getCurrentBlockStatus(any())).thenReturn(Some(mock(classOf[BlockStatus])))
    when(blockManager.getLocalBytes(serializedDataBlockId)).thenReturn(Some(makeDataBuffer()))

    val localDagScheduler = mock(classOf[LocalDagScheduler])
    when(localDagScheduler.blockManager).thenReturn(blockManager)

    taskContext = mock(classOf[TaskContextImpl])
    when(taskContext.localDagScheduler).thenReturn(localDagScheduler)
    when(taskContext.taskMetrics).thenReturn(TaskMetrics.empty)
  }

  private def makeDataBuffer(): ByteBuffer = {
    val dataSizeBytes = 1000
    val dataBuffer = ByteBuffer.allocateDirect(dataSizeBytes)
    // Sets dataBuffer's internal byte order (endianness) to the byte order used by the underlying
    // platform.
    dataBuffer.order(ByteOrder.nativeOrder())
    for (i <- 1 to dataSizeBytes) {
      dataBuffer.put(i.toByte)
    }
    dataBuffer.flip().asInstanceOf[ByteBuffer]
  }

  test("execute: invalid diskId causes failure") {
    assert(!(new DiskRemoveMonotask(taskContext, new TestBlockId("0"), "nonsense")).execute())
  }

  test("execute: actually deletes block") {
    val blockId = new TestBlockId("0")
    // Write a block to verify that it can be deleted correctly.
    val writeMonotask =
      new DiskWriteMonotask(taskContext, blockId, serializedDataBlockId, StorageLevel.DISK_ONLY)
    val diskId = "diskId"
    writeMonotask.diskId = Some(diskId)

    assert(writeMonotask.execute())
    assert(testFile.exists())
    assert(new DiskRemoveMonotask(taskContext, blockId, diskId).execute())
    assert(!testFile.exists())
  }
}

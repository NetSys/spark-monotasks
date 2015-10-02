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

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.util.Random

import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, verify, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{SparkConf, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.{BlockFileManager, BlockId, BlockManager, BlockStatus,
  MonotaskResultBlockId, TestBlockId}
import org.apache.spark.util.Utils

class DiskWriteMonotaskSuite extends FunSuite with BeforeAndAfter {

  private var blockManager: BlockManager = _
  private var taskContext: TaskContextImpl = _
  private var blockFileManager: BlockFileManager = _
  private val dataSizeBytes = 1000
  private val dataBuffer = ByteBuffer.wrap((1 to dataSizeBytes).map(_.toByte).toArray)
  private val serializedDataBlockId = new MonotaskResultBlockId(0L)

  before {
    blockFileManager = mock(classOf[BlockFileManager])
    when(blockFileManager.getBlockFile(any(), any())).thenReturn(Some(createTestFile()))

    blockManager = mock(classOf[BlockManager])
    when(blockManager.blockFileManager).thenReturn(blockFileManager)
    when(blockManager.getStatus(any())).thenReturn(Some(mock(classOf[BlockStatus])))
    when(blockManager.getLocalBytes(serializedDataBlockId)).thenReturn(Some(dataBuffer))

    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(sparkEnv)

    taskContext = mock(classOf[TaskContextImpl])
    when(taskContext.taskMetrics).thenReturn(TaskMetrics.empty)
  }

  private def createTestFile(): File = {
    // Pass in false to the SparkConf constructor so that the same configuration is loaded
    // regardless of the system properties.
    val file =
      new File(Utils.getLocalDir(new SparkConf(false)) + (new Random).nextInt(Integer.MAX_VALUE))
    file.deleteOnExit()
    file
  }

  private def verifyIllegalStateException(monotask: DiskWriteMonotask) {
    try {
      monotask.execute()
      fail(
        "This line should not have been reached because execute() should have thrown an exception.")
    } catch {
      case _: IllegalStateException => // Okay
      case _: Throwable => fail("execute() should have thrown an IllegalStateException")
    }
  }

  test("execute: throws an exception if diskId is empty") {
    verifyIllegalStateException(new DiskWriteMonotask(
      taskContext, mock(classOf[BlockId]), serializedDataBlockId))
  }

  test("execute: throws an exception if the BlockFileManager cannot provide the correct file") {
    when(blockFileManager.getBlockFile(any(), any())).thenReturn(None)
    val monotask =
      new DiskWriteMonotask(taskContext, mock(classOf[BlockId]), serializedDataBlockId)
    monotask.diskId = Some("diskId")
    verifyIllegalStateException(monotask)
  }

  test("execute: throws an exception if the serialized data cannot be found in the BlockManager") {
    when(blockManager.getLocalBytes(any())).thenReturn(None)
    val monotask = new DiskWriteMonotask(taskContext, mock(classOf[BlockId]), serializedDataBlockId)
    monotask.diskId = Some("diskId")
    verifyIllegalStateException(monotask)
  }

  test("execute: writes correct data") {
    val blockId = new TestBlockId("0")
    val monotask = new DiskWriteMonotask(taskContext, blockId, serializedDataBlockId)
    val diskId = "diskId"
    monotask.diskId = Some(diskId)

    monotask.execute()
    val fileOption = blockFileManager.getBlockFile(blockId, diskId)
    assert(fileOption.isDefined)
    val file = fileOption.get
    assert(file.exists())

    val readData = new Array[Byte](dataSizeBytes)
    val stream = new FileInputStream(file)
    var actualDataSizeBytes = 0
    var numBytesRead = 0
    while (numBytesRead != -1) {
      actualDataSizeBytes += numBytesRead
      numBytesRead = stream.read(readData)
    }
    stream.close()
    assert(actualDataSizeBytes === dataSizeBytes)
    for (i <- 0 to (dataSizeBytes - 1)) {
      assert(dataBuffer.get(i) === readData(i))
    }
  }

  test("execute: BlockManager.updateBlockInfoOnWrite() is called correctly") {
    // Do this here instead of in before() so that every block is given a different file.
    when(blockFileManager.getBlockFile(any(), any())).thenReturn(Some(createTestFile()))

    val blockId = new TestBlockId("0")
    val monotask = new DiskWriteMonotask(taskContext, blockId, serializedDataBlockId)
    val diskId = "diskId"
    monotask.diskId = Some(diskId)

    monotask.execute()
    verify(blockManager).updateBlockInfoOnWrite(blockId, diskId, dataBuffer.limit())
  }

  test("execute: verify that TaskMetrics.updatedBlocks is updated correctly") {
    // Do this here instead of in before() so that every block is given a different file.
    when(blockFileManager.getBlockFile(any(), any())).thenReturn(Some(createTestFile()))

    val monotask = new DiskWriteMonotask(taskContext, new TestBlockId("0"), serializedDataBlockId)
    monotask.diskId = Some("diskId")

    monotask.execute()
    assert(taskContext.taskMetrics.updatedBlocks.getOrElse(Seq()).size === 1)
  }
}

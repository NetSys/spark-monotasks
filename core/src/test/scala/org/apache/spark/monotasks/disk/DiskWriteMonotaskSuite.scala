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
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Random

import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, verify, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.{BlockFileManager, BlockManager, TestBlockId}
import org.apache.spark.util.Utils

class DiskWriteMonotaskSuite extends FunSuite with BeforeAndAfter {

  private var blockFileManager: BlockFileManager = _
  private var localDagScheduler: LocalDagScheduler = _
  private val numBlocks = 10
  private val dataSizeBytes = 1000
  private val dataBuffer = makeDataBuffer()

  before {
    blockFileManager = mock(classOf[BlockFileManager])

    val blockManager = mock(classOf[BlockManager])
    when(blockManager.blockFileManager).thenReturn(blockFileManager)

    val diskScheduler = mock(classOf[DiskScheduler])
    when(diskScheduler.getNextDiskId()).thenReturn("diskId")

    localDagScheduler = mock(classOf[LocalDagScheduler])
    when(localDagScheduler.diskScheduler).thenReturn(diskScheduler)
    when(localDagScheduler.blockManager).thenReturn(blockManager)
  }

  after {
    blockFileManager = null
    localDagScheduler = null
  }

  private def makeDataBuffer(): ByteBuffer = {
    val dataBuffer = ByteBuffer.allocateDirect(dataSizeBytes)
    /* Sets dataBuffer's internal byte order (endianness) to the byte order used by the underlying
     * platform. */
    dataBuffer.order(ByteOrder.nativeOrder())
    for (i <- 1 to dataSizeBytes) {
      dataBuffer.put(i.toByte)
    }
    dataBuffer.flip().asInstanceOf[ByteBuffer]
  }

  private def createTestFile(): File = {
    /* Pass in false to the SparkConf constructor so that the same configuration is loaded
     * regardless of the system properties. */
    new File(Utils.getLocalDir(new SparkConf(false)) + (new Random).nextInt(Integer.MAX_VALUE))
  }

  test("execute: BlockManager.updateBlockInfoOnWrite() is called correctly") {
    for (i <- 1 to numBlocks) {
      // Do this here instead of in before() so that every block is given a different file.
      when(blockFileManager.getBlockFile(any(), any())).thenReturn(Some(createTestFile()))

      val blockId = new TestBlockId(i.toString)
      val monotask = new DiskWriteMonotask(localDagScheduler, blockId, dataBuffer)
      val diskId = localDagScheduler.diskScheduler.getNextDiskId()
      monotask.diskId = Some(diskId)

      assert(monotask.execute())
      verify(localDagScheduler.blockManager).updateBlockInfoOnWrite(blockId, diskId)
    }
  }

  test("execute: empty diskId causes failure") {
    val monotask = new DiskWriteMonotask(localDagScheduler, new TestBlockId("0"), dataBuffer)
    assert(!monotask.execute())
  }

  test("execute: writes correct data") {
    for (i <- 1 to numBlocks) {
      // Do this here instead of in before() so that every block is given a different file.
      when(blockFileManager.getBlockFile(any(), any())).thenReturn(Some(createTestFile()))

      val blockId = new TestBlockId(i.toString)
      val monotask = new DiskWriteMonotask(localDagScheduler, blockId, dataBuffer)
      val diskId = localDagScheduler.diskScheduler.getNextDiskId()
      monotask.diskId = Some(diskId)

      assert(monotask.execute())
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
      for (j <- 0 to (dataSizeBytes - 1)) {
        assert(dataBuffer.get(j) === readData(j))
      }
    }
  }
}

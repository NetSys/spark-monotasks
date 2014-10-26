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
import org.mockito.Mockito.{mock, verify, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.{BlockFileManager, BlockId, BlockManager, MemoryStore,
  MonotaskResultBlockId, ResultWithDroppedBlocks, TestBlockId}
import org.apache.spark.util.Utils

class DiskReadMonotaskSuite extends FunSuite with BeforeAndAfter {

  private var blockFileManager: BlockFileManager = _
  private var memoryStore: MemoryStore = _
  private var localDagScheduler: LocalDagScheduler = _

  before {
    blockFileManager = mock(classOf[BlockFileManager])

    memoryStore = mock(classOf[MemoryStore])
    when(memoryStore.putValue(any(), any())).thenReturn(new ResultWithDroppedBlocks(true, null))

    val blockManager = mock(classOf[BlockManager])
    when(blockManager.blockFileManager).thenReturn(blockFileManager)
    when(blockManager.memoryStore).thenReturn(memoryStore)
    /* Pass in false to the SparkConf constructor so that the same configuration is loaded
     * regardless of the system properties. */
    when(blockManager.conf).thenReturn(new SparkConf(false))

    val diskScheduler = mock(classOf[DiskScheduler])
    when(diskScheduler.getNextDiskId()).thenReturn("diskId")

    localDagScheduler = mock(classOf[LocalDagScheduler])
    when(localDagScheduler.diskScheduler).thenReturn(diskScheduler)
    when(localDagScheduler.blockManager).thenReturn(blockManager)
  }

  after {
    blockFileManager = null
    memoryStore = null
    localDagScheduler = null
  }

  test("execute: reading nonexistent block causes failure") {
    when(blockFileManager.getBlockFile(any(), any())).thenReturn(None)
    assert(!(new DiskReadMonotask(localDagScheduler, new TestBlockId("0"), "nonsense")).execute())
  }

  test("execute: correctly stores resulting data in the MemoryStore") {
    val numBlocks = 10
    val dataSizeBytes = 1000

    val dataBuffer = ByteBuffer.allocateDirect(dataSizeBytes)
    /* Sets dataBuffer's internal byte order (endianness) to the byte order used by the underlying
     * platform. */
    dataBuffer.order(ByteOrder.nativeOrder())
    for (i <- 1 to dataSizeBytes) {
      dataBuffer.put(i.toByte)
    }
    dataBuffer.flip()

    for (i <- 1 to numBlocks) {
      /* Do this here instead of in before() so that every block is given a different file. Pass in
       * false to the SparkConf constructor so that the same configuration is loaded regardless of
       * the system properties. */
      val testFile =
        new File(Utils.getLocalDir(new SparkConf(false)) + (new Random).nextInt(Integer.MAX_VALUE))
      when(blockFileManager.getBlockFile(any(), any())).thenReturn(Some(testFile))

      val blockId = new TestBlockId(i.toString)
      // Write a block to verify that it can be read back correctly.
      val writeMonotask = new DiskWriteMonotask(localDagScheduler, blockId, dataBuffer)
      val diskId = localDagScheduler.diskScheduler.getNextDiskId()
      writeMonotask.diskId = Some(diskId)

      assert(writeMonotask.execute())
      val readMonotask = new DiskReadMonotask(localDagScheduler, blockId, diskId)
      assert(readMonotask.execute())
      val resultBlockId = new MonotaskResultBlockId(readMonotask.taskId)
      verify(memoryStore).putValue(resultBlockId, dataBuffer)
    }
  }
}

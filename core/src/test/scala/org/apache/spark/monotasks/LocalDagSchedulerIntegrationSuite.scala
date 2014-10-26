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

package org.apache.spark.monotasks

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.ArrayBuffer

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.monotasks.disk.{DiskMonotask, DiskReadMonotask, DiskRemoveMonotask,
  DiskWriteMonotask}
import org.apache.spark.storage.{BlockManager, TestBlockId}

/**
 *  This suite contains tests that verify that the LocalDagScheduler performs correctly and is
 *  stable when integrated with the various per-resource schedulers (DiskScheduler, etc.).
 */
class LocalDagSchedulerIntegrationSuite extends FunSuite with BeforeAndAfter
  with LocalSparkContext {

  private var blockManager: BlockManager = _
  private var localDagScheduler: LocalDagScheduler = _

  before {
    /* This is required because DiskMonotasks and the LocalDagScheduler take as input a
     * BlockManager, which is obtained from SparkEnv. Pass in false to the SparkConf constructor so
     * that the same configuration is loaded regardless of the system properties. */
    sc = new SparkContext("local", "test", new SparkConf(false))
    blockManager = SparkEnv.get.blockManager
    localDagScheduler = new LocalDagScheduler(blockManager)
  }

  after {
    blockManager = null
    localDagScheduler = null
  }

  test("no crashes in end-to-end operation (write, read, remove) using the DiskScheduler") {
    val timeoutMillis = 10000
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

    // Submit the write monotasks.
    val monotasks = new ArrayBuffer[DiskMonotask]()
    for (i <- 1 to numBlocks) {
      val blockId = new TestBlockId(i.toString)
      monotasks += new DiskWriteMonotask(localDagScheduler, blockId, dataBuffer)
    }
    localDagScheduler.submitMonotasks(monotasks)
    monotasks.clear()

    // Wait for the write monotasks to finish so that we can determine the blocks' diskIds.
    assert(localDagScheduler.waitUntilAllTasksComplete(timeoutMillis))

    /* Submit the read and remove monotasks. The remove tasks depend on the read tasks because we
     * want the blocks to be both read and removed, but we need the read to happen before the
     * remove. */
    for (i <- 1 to numBlocks) {
      val blockId = new TestBlockId(i.toString)
      val status = SparkEnv.get.blockManager.getStatus(blockId)
      assert(status.isDefined,
        "After a successful write, the BlockManager should have status information for this block.")
      val diskIdOption = status.get.diskId
      assert(diskIdOption.isDefined)
      val diskId = diskIdOption.get
      val readMonotask = new DiskReadMonotask(localDagScheduler, blockId, diskId)
      val removeMonotask = new DiskRemoveMonotask(localDagScheduler, blockId, diskId)
      removeMonotask.addDependency(readMonotask)
      monotasks += readMonotask
      monotasks += removeMonotask
    }
    localDagScheduler.submitMonotasks(monotasks)

    // Wait for the read and remove monotasks to finish.
    assert(localDagScheduler.waitUntilAllTasksComplete(timeoutMillis))
  }
}

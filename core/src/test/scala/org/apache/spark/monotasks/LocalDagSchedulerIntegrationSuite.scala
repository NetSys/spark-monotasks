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

import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.{ExecutorBackend, TaskMetrics}
import org.apache.spark.monotasks.disk.{DiskMonotask, DiskReadMonotask, DiskRemoveMonotask,
  DiskWriteMonotask}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

/**
 *  This suite contains tests that verify that the LocalDagScheduler performs correctly and is
 *  stable when integrated with the various per-resource schedulers (DiskScheduler, etc.).
 */
class LocalDagSchedulerIntegrationSuite extends FunSuite with BeforeAndAfter
  with LocalSparkContext {

  private var taskContext: TaskContextImpl = _
  private var blockManager: BlockManager = _
  private var localDagScheduler: LocalDagScheduler = _

  before {
    // This is required because the LocalDagScheduler takes as input a BlockManager, which is
    // obtained from SparkEnv. Pass in false to the SparkConf constructor so that the same
    // configuration is loaded regardless of the system properties.
    sc = new SparkContext("local", "test", new SparkConf(false))
    blockManager = SparkEnv.get.blockManager
    localDagScheduler = new LocalDagScheduler(mock(classOf[ExecutorBackend]), blockManager)

    taskContext = mock(classOf[TaskContextImpl])
    when(taskContext.env).thenReturn(SparkEnv.get)
    when(taskContext.localDagScheduler).thenReturn(localDagScheduler)
    when(taskContext.taskMetrics).thenReturn(TaskMetrics.empty)
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
      val serializedDataBlockId = new MonotaskResultBlockId(i)
      blockManager.cacheBytes(serializedDataBlockId, dataBuffer, StorageLevel.MEMORY_ONLY_SER, false)
      val blockId = new TestBlockId(i.toString)
      monotasks +=
        new DiskWriteMonotask(taskContext, blockId, serializedDataBlockId, StorageLevel.DISK_ONLY)
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
      val readMonotask = new DiskReadMonotask(taskContext, blockId, diskId)
      val removeMonotask = new DiskRemoveMonotask(taskContext, blockId, diskId)
      removeMonotask.addDependency(readMonotask)
      monotasks += readMonotask
      monotasks += removeMonotask
    }
    localDagScheduler.submitMonotasks(monotasks)

    // Wait for the read and remove monotasks to finish.
    assert(localDagScheduler.waitUntilAllTasksComplete(timeoutMillis))
  }

  test("all temporary blocks are deleted from the BlockManager") {
    val numRddItems = 100
    val rddA = sc.parallelize(1 to numRddItems).persist(StorageLevel.MEMORY_ONLY)
    val rddB = rddA.map(x => (x, x)).persist(StorageLevel.DISK_ONLY)
    val rddC = rddB.map(y => (y, y)).persist(StorageLevel.MEMORY_ONLY)
    assert(rddC.count() === numRddItems)

    // Returns a sequence of the BlockIds corresponding to the provided RDD's partitions.
    def getBlockIds(rdd: RDD[_]): Seq[BlockId] = {
      val id = rdd.id
      rdd.partitions.map(partition => new RDDBlockId(id, partition.index))
    }

    var rddBlockIds = getBlockIds(rddA) ++ getBlockIds(rddC)
    val memoryStore = blockManager.memoryStore
    var nonRddBlockIds = memoryStore.getAllBlockIds().filter(!rddBlockIds.contains(_))
    assert(nonRddBlockIds.filter(!_.isInstanceOf[BroadcastBlockId]).size === 0)

    // Construct a DAG that contains a DiskReadMonotask.
    val rddD = rddB.map(z => (z, z))
    assert(rddD.count() === numRddItems)

    rddBlockIds = getBlockIds(rddA) ++ getBlockIds(rddC)
    nonRddBlockIds = memoryStore.getAllBlockIds().filter(!rddBlockIds.contains(_))
    assert(nonRddBlockIds.filter(!_.isInstanceOf[BroadcastBlockId]).size === 0)
  }
}

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

import java.nio.ByteBuffer

import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.monotasks.disk.{DiskReadMonotask, DiskRemoveMonotask, DiskWriteMonotask}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage._

/**
 *  This suite contains tests that verify that the LocalDagScheduler performs correctly and is
 *  stable when integrated with the various per-resource schedulers (DiskScheduler, etc.).
 */
class LocalDagSchedulerIntegrationSuite extends FunSuite with BeforeAndAfter
  with LocalSparkContext {

  private var blockManager: BlockManager = _
  private var conf: SparkConf = _

  // Use a wrapped version of LocalDagScheduler so we can run events sychronously.
  private var localDagScheduler: LocalDagSchedulerWithSynchrony = _

  before {
    // Pass in false to the SparkConf constructor so that the same configuration is loaded
    // regardless of the system properties.
    conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)
    blockManager = SparkEnv.get.blockManager
  }

  /** Sets up the SparkEnv with a special, synchronized local dag scheduler. */
  private def setupLocalDagSchedulerWithSynchrony(): Unit = {
    localDagScheduler = new LocalDagSchedulerWithSynchrony(
      mock(classOf[ExecutorBackend]), blockManager.blockFileManager)


    // Set a new SparkEnv that points to the synchronous LocalDagScheduler.
    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.localDagScheduler).thenReturn(localDagScheduler)
    when(sparkEnv.blockManager).thenReturn(blockManager)
    when(sparkEnv.closureSerializer).thenReturn(new JavaSerializer(conf))
    when(sparkEnv.conf).thenReturn(conf)
    when(sparkEnv.broadcastManager).thenReturn(SparkEnv.get.broadcastManager)
    when(sparkEnv.serializer).thenReturn(new JavaSerializer(conf))
    when(sparkEnv.dependencyManager).thenReturn(SparkEnv.get.dependencyManager)
    when(sparkEnv.mapOutputTracker).thenReturn(SparkEnv.get.mapOutputTracker)
    SparkEnv.set(sparkEnv)
  }

  test("no crashes in end-to-end operation (write, read, remove) using the DiskScheduler") {
    setupLocalDagSchedulerWithSynchrony()
    val timeoutMillis = 10000
    val numBlocks = 10
    val dataBuffer = ByteBuffer.wrap((1 to 1000).map(_.toByte).toArray)
    val taskContext = new TaskContextImpl(0, 0)

    // Submit the write monotasks. Create a result monotask that creates a dummy task result and
    // saves that as the macrotask result, so that the LocalDagScheduler will identify the macrotask
    // associated with these monotasks as finished.
    val writeResultMonotask = new SimpleMonotaskWithMacrotaskResult(blockManager, taskContext)
    val writeMonotasks = (1 to numBlocks).map { i =>
      val serializedDataBlockId = new MonotaskResultBlockId(i)
      blockManager.cacheBytes(
        serializedDataBlockId, dataBuffer, StorageLevel.MEMORY_ONLY_SER, false)
      val blockId = new TestBlockId(i.toString)
      val diskWriteMonotask = new DiskWriteMonotask(taskContext, blockId, serializedDataBlockId)
      writeResultMonotask.addDependency(diskWriteMonotask)
      diskWriteMonotask
    }

    localDagScheduler.runEvent(SubmitMonotasks(writeMonotasks))
    localDagScheduler.runEvent(SubmitMonotask(writeResultMonotask))

    // Wait for the write monotasks to finish so that we can determine the blocks' diskIds.
    assert(localDagScheduler.waitUntilAllMacrotasksComplete(timeoutMillis))

    /* Submit the read and remove monotasks. The remove tasks depend on the read tasks because we
     * want the blocks to be both read and removed, but we need the read to happen before the
     * remove. */
     val readAndRemoveResultMonotask =
      new SimpleMonotaskWithMacrotaskResult(blockManager, taskContext)
     val readAndRemoveMonotasks = (1 to numBlocks).flatMap { i =>
      val blockId = new TestBlockId(i.toString)
      val status = SparkEnv.get.blockManager.getStatus(blockId)
      assert(status.isDefined, "After a successful write, the BlockManager should have status " +
        s"information for block $blockId.")
      val diskIdOption = status.get.diskId
      assert(diskIdOption.isDefined)
      val diskId = diskIdOption.get
      val readMonotask = new DiskReadMonotask(taskContext, blockId, diskId)
      val removeMonotask = new DiskRemoveMonotask(taskContext, blockId, diskId)
      removeMonotask.addDependency(readMonotask)
      readAndRemoveResultMonotask.addDependency(removeMonotask)
      List(readMonotask, removeMonotask)
    }

    localDagScheduler.runEvent(SubmitMonotasks(readAndRemoveMonotasks))
    localDagScheduler.runEvent(SubmitMonotask(readAndRemoveResultMonotask))

    // Wait for the read and remove monotasks to finish.
    assert(localDagScheduler.waitUntilAllMacrotasksComplete(timeoutMillis))
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

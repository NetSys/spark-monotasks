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
import java.util.Map.Entry

import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.{BlockFileManager, BlockManager, TestBlockId}
import org.apache.spark.util.Utils

class DiskSchedulerSuite extends FunSuite with BeforeAndAfter {

  private var blockManager: BlockManager = _
  private var localDagScheduler: LocalDagScheduler = _
  val timeoutMillis = 10000
  val numBlocks = 10

  before {
    blockManager = mock(classOf[BlockManager])
    DummyDiskMonotask.clearTimes()
  }

  after {
    blockManager = null
    localDagScheduler = null
  }

  private def initializeLocalDagScheduler(numDisks: Int) {
    /* Pass in false to the SparkConf constructor so that the same configuration is loaded
     * regardless of the system properties. */
    val conf = new SparkConf(false)
    if (numDisks > 1) {
      /* Create numDisks sub-directories in the current Spark local directory and make them the
       * Spark local directories. */
      val oldLocalDir = Utils.getLocalDir(conf)
      val newLocalDirs = List.range(0, numDisks).map(i => oldLocalDir + i.toString).mkString(",")
      conf.set("spark.local.dir", newLocalDirs)
    }
    when(blockManager.blockFileManager).thenReturn(new BlockFileManager(conf))
    localDagScheduler = new LocalDagScheduler(blockManager)
  }

  test("when using one disk, at most one DiskMonotask is executed at a time") {
    initializeLocalDagScheduler(1)

    val monotasks = new ArrayBuffer[DummyDiskMonotask]()
    for (i <- 1 to numBlocks) {
      val blockId = new TestBlockId(i.toString)
      monotasks += new DummyDiskMonotask(localDagScheduler, blockId, 100)
    }
    localDagScheduler.submitMonotasks(monotasks)

    assert(localDagScheduler.waitUntilAllTasksComplete(timeoutMillis))
  }

  test("when using multiple disks, at most one DiskMonotask is executed at a time per disk") {
    initializeLocalDagScheduler(2)

    val monotasks = new ArrayBuffer[DummyDiskMonotask]()
    for (i <- 1 to numBlocks) {
      val blockId = new TestBlockId(i.toString)
      monotasks += new DummyDiskMonotask(localDagScheduler, blockId, 100)
    }
    localDagScheduler.submitMonotasks(monotasks)

    assert(localDagScheduler.waitUntilAllTasksComplete(timeoutMillis))
  }

  test("DiskMonotasks pertaining to the same disk are executed in FIFO order") {
    initializeLocalDagScheduler(1)

    val monotasks = new ArrayBuffer[DummyDiskMonotask]()
    for (i <- 1 to numBlocks) {
      val blockId = new TestBlockId(i.toString)
      monotasks += new DummyDiskMonotask(localDagScheduler, blockId, 100)
    }
    localDagScheduler.submitMonotasks(monotasks)

    assert(localDagScheduler.waitUntilAllTasksComplete(timeoutMillis))
    val ids = monotasks.map(monotask => monotask.taskId)
    // Sort the tasks by start time, and extract the taskIds.
    val timesArray = DummyDiskMonotask.taskTimes.entrySet.toArray(Array[Entry[Long, Long]]())
    val sortedIds = timesArray.sortWith((a, b) => b.getValue() >= a.getValue()).map(a => a.getKey())
    assert(ids.sameElements(sortedIds))
  }
}

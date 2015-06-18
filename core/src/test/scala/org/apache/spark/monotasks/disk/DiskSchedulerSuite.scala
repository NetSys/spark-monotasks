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

import java.util.Map.Entry

import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{SparkConf, TaskContextImpl}
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.storage.{BlockFileManager, BlockManager, TestBlockId}
import org.apache.spark.util.Utils

class DiskSchedulerSuite extends FunSuite with BeforeAndAfter {

  private var blockManager: BlockManager = _
  private var diskScheduler: DiskScheduler = _
  private var taskContext: TaskContextImpl = _
  val timeoutMillis = 10000
  val numBlocks = 10

  before {
    blockManager = mock(classOf[BlockManager])
    DummyDiskMonotask.clearTimes()

    val localDagScheduler = mock(classOf[LocalDagScheduler])
    taskContext = mock(classOf[TaskContextImpl])
    when(taskContext.localDagScheduler).thenReturn(localDagScheduler)
    when(localDagScheduler.blockManager).thenReturn(blockManager)
  }

  private def initializeDiskScheduler(numDisks: Int) {
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
    diskScheduler = new DiskScheduler(blockManager)
  }

  test("when using one disk, at most one DiskMonotask is executed at a time") {
    initializeDiskScheduler(1)

    val monotasks = (1 to numBlocks).map { i =>
      val blockId = new TestBlockId(i.toString)
      new DummyDiskMonotask(taskContext, blockId, 100)
    }
    assert(submitTasksAndWaitForCompletion(monotasks, timeoutMillis))
  }

  test("when using multiple disks, at most one DiskMonotask is executed at a time per disk") {
    initializeDiskScheduler(2)

    val monotasks = (1 to numBlocks).map { i =>
      val blockId = new TestBlockId(i.toString)
      new DummyDiskMonotask(taskContext, blockId, 100)
    }
    assert(submitTasksAndWaitForCompletion(monotasks, timeoutMillis))
  }

  test("DiskMonotasks pertaining to the same disk are executed in FIFO order") {
    initializeDiskScheduler(1)

    val monotasks = (1 to numBlocks).map { i =>
      val blockId = new TestBlockId(i.toString)
      new DummyDiskMonotask(taskContext, blockId, 100)
    }
    assert(submitTasksAndWaitForCompletion(monotasks, timeoutMillis))

    val ids = monotasks.map(monotask => monotask.taskId)
    // Sort the tasks by start time, and extract the taskIds.
    val timesArray = DummyDiskMonotask.taskTimes.entrySet.toArray(Array[Entry[Long, Long]]())
    val sortedIds = timesArray.sortWith((a, b) => b.getValue() >= a.getValue()).map(a => a.getKey())
    assert(ids.sameElements(sortedIds))
  }

  private def submitTasksAndWaitForCompletion(
      monotasks: Seq[DummyDiskMonotask], timeoutMillis: Long): Boolean = {
    monotasks.foreach { diskScheduler.submitTask(_) }
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (monotasks.count(!_.isFinished) > 0) {
      if (System.currentTimeMillis > finishTime) {
        return false
      }

      Thread.sleep(10)
    }
    true
  }
}

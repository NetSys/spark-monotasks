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

import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet

import org.mockito.ArgumentMatcher
import org.mockito.Matchers.argThat
import org.mockito.Mockito.{mock, timeout, verify, when}

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkEnv, TaskContextImpl}
import org.apache.spark.monotasks.{LocalDagScheduler, TaskFailure, TaskSuccess}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockFileManager, BlockId, TestBlockId}
import org.apache.spark.util.Utils

class DiskSchedulerSuite extends FunSuite with BeforeAndAfter with Timeouts {
  private var diskScheduler: DiskScheduler = _
  private val taskContext: TaskContextImpl = new TaskContextImpl(0, 0)
  private var localDagScheduler: LocalDagScheduler = _
  private var conf: SparkConf = _
  private val timeoutMillis = 20000
  private val numBlocks = 100


  before {
    DiskMonotaskTestHelper.reset()

    // Pass in false to the SparkConf constructor so that the same configuration is loaded
    // regardless of the system properties.
    conf = new SparkConf(false)

    // Set a SparkEnv with a (mocked) LocalDagScheduler so we can verify the interactions with it.
    localDagScheduler = mock(classOf[LocalDagScheduler])
    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.localDagScheduler).thenReturn(localDagScheduler)
    when(sparkEnv.closureSerializer).thenReturn(new JavaSerializer(conf))
    SparkEnv.set(sparkEnv)
  }

  private def initializeDiskScheduler(numDisks: Int, numThreadsPerDisk: Int = 1) {
    if (numDisks > 1) {
      /* Create numDisks sub-directories in the current Spark local directory and make them the
       * Spark local directories. */
      val oldLocalDir = Utils.getLocalDir(conf)
      val newLocalDirs = (1 to numDisks).map(i => oldLocalDir + i.toString).mkString(",")
      conf.set("spark.local.dir", newLocalDirs)
    }
    conf.set("spark.monotasks.threadsPerDisk", numThreadsPerDisk.toString())
    diskScheduler = new DiskScheduler(new BlockFileManager(conf), conf)
  }

  test("submitTask: fails a DiskMonotask if its diskId is invalid") {
    initializeDiskScheduler(1)

    val monotask = new DiskReadMonotask(taskContext, mock(classOf[BlockId]), "nonsense")
    diskScheduler.submitTask(monotask)
    verify(localDagScheduler).post(argThat(new TaskFailureContainsMonotask(monotask)))
  }

  test("the correct number of tasks are executed at a time per disk") {
    val numThreadsPerDisk = 5
    initializeDiskScheduler(5, numThreadsPerDisk)

    val monotasks = (1 to numBlocks).map(i =>
      new DummyDiskWriteMonotask(taskContext, new TestBlockId(i.toString), 100L, numThreadsPerDisk))
    assert(submitTasksAndWaitForCompletion(monotasks, timeoutMillis))

    DiskMonotaskTestHelper.maxTasksPerDisk.foreach { case (diskId, maxTasksPerDisk) =>
      assert(
        maxTasksPerDisk === numThreadsPerDisk,
        s"Disk $diskId: $numThreadsPerDisk tasks should have executed concurrently, but " +
          s"$maxTasksPerDisk actually executed concurrently.")
    }
  }

  test("DiskMonotasks for the same disk and of the same type execute in FIFO order") {
    initializeDiskScheduler(1)
    val monotasks = (1 to numBlocks).map(i =>
      new DummyDiskWriteMonotask(taskContext, new TestBlockId(i.toString), 100))
    assert(submitTasksAndWaitForCompletion(monotasks, timeoutMillis))

    val ids = monotasks.map(monotask => monotask.taskId)
    // Sort the tasks by start time, and extract the taskIds.
    val timesArray = DiskMonotaskTestHelper.finishedTaskIdToStartTimeMillis.entrySet.toArray(
      Array[Entry[Long, Long]]())
    val sortedIds = timesArray.sortWith((a, b) => b.getValue() >= a.getValue()).map(a => a.getKey())
    assert(ids.sameElements(sortedIds))
  }

  test("the LocalDagScheduler is notified when a DiskMonotask completes successfully") {
    initializeDiskScheduler(1)
    val monotask = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 0L)
    diskScheduler.submitTask(monotask)
    verify(localDagScheduler, timeout(timeoutMillis)).post(TaskSuccess(monotask, None))
  }

  test("the LocalDagScheduler is notified when a DiskMonotask fails") {
    initializeDiskScheduler(1)

    val monotask = new DiskWriteMonotask(
      taskContext, mock(classOf[BlockId]), mock(classOf[BlockId])) {
        override def execute() { throw new Exception("Error!") }
    }
    diskScheduler.submitTask(monotask)

    verify(localDagScheduler, timeout(timeoutMillis)).post(
      argThat(new TaskFailureContainsMonotask(monotask)))
  }

  test("interrupting a DiskAccessor thread prevents queued DiskMonotasks from being executed") {
    initializeDiskScheduler(1)

    val idsOfStartedMonotasks = new HashSet[Long]()
    var diskAccessorThreadId = 0L
    // This monotask will interrupt the DiskAccessor thread that is executing it.
    val firstMonotask = new DiskWriteMonotask(
      taskContext, mock(classOf[BlockId]), mock(classOf[BlockId])) {
        override def execute() {
          idsOfStartedMonotasks += taskId
          diskAccessorThreadId = Thread.currentThread().getId()

          // This will cause the DiskAccessor thread to expire.
          Thread.currentThread().interrupt()
        }
      }
    // These monotasks will be queued in the DiskAccessor when it is interrupted, so they should not
    // be executed.
    val otherMonotasks = (1 to 10).map { i =>
      new DiskWriteMonotask(taskContext, mock(classOf[BlockId]), mock(classOf[BlockId])) {
        override def execute() { idsOfStartedMonotasks += taskId }
      }
    }
    (Seq(firstMonotask) ++ otherMonotasks).foreach(diskScheduler.submitTask(_))

    failAfter(timeoutMillis millis) {
      // Loop until the DiskAccessor thread is no longer running.
      var allThreadIds: Seq[Long] = Seq.empty
      do {
        Thread.sleep(10L)
        allThreadIds = Thread.getAllStackTraces().keySet().map(_.getId()).toSeq
      } while (allThreadIds.contains(diskAccessorThreadId))
    }

    // Once the DiskAccessor thread has been interrupted, no more DiskMonotasks should be executed.
    // Only "firstMonotask" should have started executing before the DiskAccessor thread stopped.
    assert(idsOfStartedMonotasks.contains(firstMonotask.taskId))
    otherMonotasks.foreach(monotask => assert(!idsOfStartedMonotasks.contains(monotask.taskId)))
  }

  test("diskWriteMonotasks are scheduled based on the next available disk when load balanced") {
    conf.set("spark.monotasks.loadBalanceDiskWrites", "true")

    initializeDiskScheduler(2)
    val disk0 = diskScheduler.diskIds(0)
    val disk1 = diskScheduler.diskIds(1)
    val longMonotask = new DummyDiskReadMonotask(taskContext, mock(classOf[BlockId]), disk0, 300)
    val shortMonotask = new DummyDiskReadMonotask(taskContext, mock(classOf[BlockId]), disk1, 100)
    val writeMonotask = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 400)
    val shortWrite1 = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 50)
    val shortWrite2 = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 50)

    // Since disk1 will be available first, the writeMonotask should be assigned to disk1.
    // Then the two short writes should be assigned to disk0, which will become available while
    // the writeMonotask is running.
    val monotasks = Seq(longMonotask, shortMonotask, writeMonotask, shortWrite1, shortWrite2)

    assert(submitTasksAndWaitForCompletion(monotasks, 1000))
    assert(writeMonotask.diskId.isDefined)
    assert(shortWrite1.diskId.isDefined)
    assert(shortWrite2.diskId.isDefined)
    assert(writeMonotask.diskId.get === disk1)
    assert(shortWrite1.diskId.get === disk0)
    assert(shortWrite2.diskId.get === disk0)
  }

  test("diskWriteMonotasks are scheduled based on round robin when not load balanced") {
    conf.set("spark.monotasks.loadBalanceDiskWrites", "false")

    initializeDiskScheduler(2)
    val disk0 = diskScheduler.diskIds(0)
    val disk1 = diskScheduler.diskIds(1)
    val longMonotask = new DummyDiskReadMonotask(taskContext, mock(classOf[BlockId]), disk0, 300)
    val shortMonotask = new DummyDiskReadMonotask(taskContext, mock(classOf[BlockId]), disk1, 100)
    val writeMonotask = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 400)
    val shortWrite1 = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 50)
    val shortWrite2 = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 50)

    val monotasks = Seq(longMonotask, shortMonotask, writeMonotask, shortWrite1, shortWrite2)
    assert(submitTasksAndWaitForCompletion(monotasks, 1000))
    assert(writeMonotask.diskId.isDefined)
    assert(shortWrite1.diskId.isDefined)
    assert(shortWrite2.diskId.isDefined)
    assert(writeMonotask.diskId.get === disk0)
    assert(shortWrite1.diskId.get === disk1)
    assert(shortWrite2.diskId.get === disk0)
  }

  test("diskAccessors count 0 monotasks in queue after all diskWriteMonotasks have finished") {
    initializeDiskScheduler(2)
    val writeMonotask1 = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 100)
    val writeMonotask2 = new DummyDiskWriteMonotask(taskContext, mock(classOf[BlockId]), 100)

    val monotasks = Seq(writeMonotask1, writeMonotask2)
    submitTasksAndWaitForCompletion(monotasks, 1000)

    val diskNames = diskScheduler.diskIds.map(BlockFileManager.getDiskNameFromPath(_))
    for (diskName <- diskNames) {
      assert(diskScheduler.getDiskNameToNumRunningAndQueuedDiskMonotasks(diskName)._1 === 0)
    }
  }

  private def submitTasksAndWaitForCompletion(
      monotasks: Seq[DiskMonotask], timeoutMillis: Long): Boolean = {
    monotasks.foreach { diskScheduler.submitTask(_) }
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (monotasks.size > DiskMonotaskTestHelper.finishedTaskIdToStartTimeMillis.size) {
      if (System.currentTimeMillis > finishTime) {
        return false
      }

      Thread.sleep(10)
    }
    true
  }

  /**
   * This is a custom matcher that ensures that the task failure includes the provided monotask and
   * that the failure reason is non-null.
   */
  private class TaskFailureContainsMonotask(monotask: DiskMonotask)
    extends ArgumentMatcher[TaskFailure] {

    override def matches(o: Object): Boolean = o match {
      case failure: TaskFailure =>
        (failure.failedMonotask == monotask) && (failure.serializedFailureReason.isDefined)
      case _ =>
        false
    }
  }
}

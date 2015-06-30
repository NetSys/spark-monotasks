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
  private val taskContext: TaskContextImpl = new TaskContextImpl(0, 0, 0)
  private var localDagScheduler: LocalDagScheduler = _
  private var conf: SparkConf = _
  val timeoutMillis = 10000
  val numBlocks = 10

  before {
    DummyDiskMonotask.clearTimes()

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

  private def initializeDiskScheduler(numDisks: Int) {
    if (numDisks > 1) {
      /* Create numDisks sub-directories in the current Spark local directory and make them the
       * Spark local directories. */
      val oldLocalDir = Utils.getLocalDir(conf)
      val newLocalDirs = List.range(0, numDisks).map(i => oldLocalDir + i.toString).mkString(",")
      conf.set("spark.local.dir", newLocalDirs)
    }
    diskScheduler = new DiskScheduler(new BlockFileManager(conf))
  }

  test("submitTask: fails a DiskMonotask if its diskId is invalid") {
    initializeDiskScheduler(1)

    val monotask = new DiskReadMonotask(taskContext, mock(classOf[BlockId]), "nonsense")
    diskScheduler.submitTask(monotask)
    verify(localDagScheduler).post(argThat(new TaskFailureContainsMonotask(monotask)))
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

  test("the LocalDagScheduler is notified when a DiskMonotask completes successfully") {
    initializeDiskScheduler(1)
    val monotask = new DummyDiskMonotask(taskContext, mock(classOf[BlockId]), 0L)
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

  /**
   * This is a custom matcher that ensures that the task failure includes the provided monotask and
   * that the failure reason is non-null.
   */
  private class TaskFailureContainsMonotask(monotask: DiskMonotask)
    extends ArgumentMatcher[TaskFailure] {

    override def matches(o: Object): Boolean = o match {
      case failure: TaskFailure =>
        (failure.failedMonotask == monotask) && (failure.serializedFailureReason != null)
      case _ =>
        false
    }
  }
}

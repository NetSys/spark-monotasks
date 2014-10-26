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

import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.HashMap

import org.apache.spark.Logging
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils

/**
 * Schedules DiskMonotasks for execution on the appropriate disk. If the DiskMonotask is writing a
 * block, then the DiskScheduler determines which disk that block should be written to.
 * Maintains a set of threads, one for each disk, that execute DiskMonotasks.
 */
private[spark] class DiskScheduler(blockManager: BlockManager) extends Logging {

  // Mapping from disk identifier to the corresponding DiskAccessor object.
  private val diskAccessors = new HashMap[String, DiskAccessor]()
  // Mapping from disk identifier to the corresponding DiskAccessor's thread.
  private val diskAccessorThreads = new HashMap[String, Thread]()
  /* A list of unique identifiers for all of this worker's physical disks. Assumes that each path in
   * the user-defined list of Spark local directories (spark.local.dirs or SPARK_LOCAL_DIRS, if it
   * is set) describes a separate physical disk. */
  val diskIds = blockManager.blockFileManager.localDirs.keySet.toArray
  // An index into diskIds indicating which disk to use for the next write task.
  private var nextDisk: Int = 0

  addShutdownHook()
  buildDiskAccessors()

  /**
   * Builds a DiskAccessor object for each disk identifier, and starts a thread for each new
   * DiskAccessor object.
   */
  private def buildDiskAccessors() {
    diskIds.foreach { diskId =>
      val diskAccessor = new DiskAccessor()
      diskAccessors(diskId) = diskAccessor
      val diskAccessorThread = new Thread(diskAccessor)
      diskAccessorThread.setDaemon(true)
      diskAccessorThread.setName(s"DiskAccessor thread for disk $diskId")
      diskAccessorThread.start()
      diskAccessorThreads(diskId) = diskAccessorThread
    }
  }

  /**
   * Looks up the disk on which the provided DiskMonotask will operate, and adds it to that disk's
   * task queue.
   */
  def submitTask(task: DiskMonotask) {
    /* Extract the identifier of the disk on which this task will operate. If the task is a write,
     * a new disk identifier must be created. */
    val diskId: Option[String] = task match {
      case write: DiskWriteMonotask => {
        val id = Some(getNextDiskId())
        write.diskId = id
        id
      }
      case read: DiskReadMonotask => {
        Some(read.diskId)
      }
      case remove: DiskRemoveMonotask => {
        Some(remove.diskId)
      }
      case _ => {
        None
      }
    }
    if (diskId.isEmpty) {
      logError(s"DiskMonotask ${task.taskId} rejected because its subtype is not supported.")
      // TODO: Tell the LocalDagScheduler that this DiskMonotask failed.
      task.localDagScheduler.handleTaskCompletion(task)
    } else {
      diskAccessors(diskId.get).taskQueue.add(task)
    }
  }

  /**
   *  Returns the diskId of the disk that should be used for the next write operation. Uses a
   *  round-robin disk scheduling strategy.
   */
  def getNextDiskId(): String = {
    val diskId = diskIds(nextDisk)
    nextDisk = (nextDisk + 1) % diskIds.length
    diskId
  }

  private def addShutdownHook() {
    Runtime.getRuntime.addShutdownHook(new Thread("Stop DiskAccessor threads") {
      override def run() = Utils.logUncaughtExceptions {
        DiskScheduler.this.stop()
      }
    })
  }

  /**
   * Interrupts all of the DiskAccessor threads, causing them to stop executing tasks and expire.
   * During normal operation, this method will be called once all tasks have been executed and the
   * DiskScheduler is ready to shut down. In that case, all of the DiskAccessor threads will be
   * interrupted while waiting for more tasks to be added to the task queue. This is harmless.
   * However, if any DiskAccessor threads are still executing tasks, this will interrupt them
   * before they finish.
   */
  def stop() {
    diskAccessorThreads.values.foreach { diskAccessorThread =>
      val threadName = diskAccessorThread.getName()
      logDebug(s"Attempting to join thread: $threadName")
      diskAccessorThread.interrupt()
      diskAccessorThread.join()
      logDebug(s"Successfully joined thread: $threadName")
    }
  }

  private def getTotalQueueLength(): Int = {
    diskAccessors.values.map(diskAccessor => diskAccessor.taskQueue.size).sum
  }

  /**
   * Stores a single disk's task queue. When used to create a thread, a DiskAccessor object will
   * execute the DiskMonotasks in its task queue.
   */
  private class DiskAccessor() extends Runnable {

    // A queue of DiskMonotasks that are waiting to be executed.
    val taskQueue = new LinkedBlockingQueue[DiskMonotask]()

    /** Continuously executes DiskMonotasks from the task queue. */
    def run() {
      try {
        while (true) {
          val task = taskQueue.take()
          if (task.execute()) {
            logDebug(s"Monotask ${task.taskId} succeeded.")
            task.localDagScheduler.handleTaskCompletion(task)
          } else {
            logError(s"Monotask ${task.taskId} failed.")
            // TODO: Tell the LocalDagScheduler that this DiskMonotask failed.
            task.localDagScheduler.handleTaskCompletion(task)
          }
        }
      } catch {
        case _: InterruptedException => {
          logDebug(s"Thread interrupted while running: ${Thread.currentThread().getName()}")
        }
        case _: ClosedByInterruptException => {
          /* Happens when this DiskAccessor is interrupted while blocked on an I/O operation upon a
           * channel. The channel will be closed. This should happen very rarely. */
          logDebug(s"Thread interrupted while waiting: ${Thread.currentThread().getName()}")
        }
      }
    }
  }
}

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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.apache.spark.{Logging, SparkConf, SparkEnv, SparkException}
import org.apache.spark.monotasks.TaskSuccess
import org.apache.spark.storage.BlockFileManager
import org.apache.spark.util.{SparkUncaughtExceptionHandler, Utils}

/**
 * Schedules DiskMonotasks for execution on the appropriate disk. If the DiskMonotask is writing a
 * block, then the DiskScheduler determines which disk that block should be written to. The
 * configuration parameter "spark.monotasks.threadsPerDisk" determines the maximum number of
 * DiskMonotasks that will execute concurrently on each disk.
 */
private[spark] class DiskScheduler(
    blockFileManager: BlockFileManager,
    conf: SparkConf) extends Logging {

  // Mapping from disk identifier to the corresponding DiskAccessor object.
  private val diskAccessors = new HashMap[String, DiskAccessor]()
  /* A list of unique identifiers for all of this worker's physical disks. Assumes that each path in
   * the user-defined list of Spark local directories (spark.local.dirs or SPARK_LOCAL_DIRS, if it
   * is set) describes a separate physical disk. */
  val diskIds = blockFileManager.localDirs.keySet.toArray
  /**
   * Queue of HdfsWriteMonotasks that have been submitted to the scheduler but haven't yet been
   * submitted to a DiskAccessor, because we first need to determine which disk the monotask will
   * access.
   */
  private val hdfsWriteMonotaskQueue = new LinkedBlockingQueue[HdfsWriteMonotask]()
  /**
   * An index into diskIds indicating which disk to use for the next write task. This is only used
   * if loadBalanceDiskWrites is false.
   */
  private var nextDisk = 0
  /**
   * True if DiskWriteMonotasks should be load balanced across available disks based on the speed
   * of the disk; otherwise, assigns DiskWriteMonotasks to disks in round-robin order.
   */
  private val loadBalanceDiskWrites = conf.getBoolean(
    "spark.monotasks.loadBalanceDiskWrites", false)

  addShutdownHook()
  buildDiskAccessors(conf)
  launchHdfsWriteMonotaskSubmitterThread()

  /**
   * Builds a DiskAccessor object with a user-defined concurrency for each disk identifier.
   *
   * TODO: Currently, all DiskAccessors are created with the same degree of concurrency. It may be
   *       interesting to implement a more advanced design that allows the user to specify a
   *       different concurrency for each disk. This would be useful for systems that use both SSDs
   *       and HDDs.
   */
  private def buildDiskAccessors(conf: SparkConf) {
    val numThreadsPerDisk = conf.getInt("spark.monotasks.threadsPerDisk", 1)
    diskIds.foreach { diskId =>
      logInfo(s"Creating $numThreadsPerDisk threads for disk $diskId")
      diskAccessors(diskId) = new DiskAccessor(diskId, numThreadsPerDisk)
    }
  }

  /**
   * Launches a daemon thread responsible for determining which disk HdfsWriteMonotasks should run
   * on. This is done in a separate thread, rather than in the scheduler's main thread, because it
   * requires writing a small amount of data to disk, so it takes a non-trivial amount of time.
   *
   * This small write may interfere with disk monotasks that are currently running; we can't avoid
   * this because we don't know what disk these monotasks will run on until we've written a small
   * amount of data.  To mitigate the effect of this, we don't sync these writes to disk, so they
   * should typically remain in the buffer cache.
   */
  private def launchHdfsWriteMonotaskSubmitterThread(): Unit = {
    val submitter = new Thread(new Runnable() {
      override def run(): Unit = {
        while (true) {
          val hdfsWriteMonotask = hdfsWriteMonotaskQueue.take()

          try {
            hdfsWriteMonotask.initialize()
            addToDiskQueue(hdfsWriteMonotask, hdfsWriteMonotask.chooseLocalDir(diskIds))
          } catch {
            case NonFatal(e) => hdfsWriteMonotask.handleException(e)
          }
        }
      }
    })
    submitter.setDaemon(true)
    submitter.setName("HdfsWriteMonotask submitter")
    submitter.start()
  }

  /**
   * Returns a mapping of physical disk name to the number of running and queued disk monotasks on
   * that disk.  The first count is the number of running and queued disk monotasks (total), the
   * next is the number of queued read monotasks, then the number of queued remove monotasks, and
   * finally the number of queued write monotasks.
   */
  def getDiskNameToNumRunningAndQueuedDiskMonotasks: HashMap[String, (Int, Int, Int, Int)] = {
    diskAccessors.map {
      case (_, diskAccessor) =>
        (diskAccessor.diskName, diskAccessor.getNumRunningAndQueuedDiskMonotasks)
    }
  }

  /**
   * Looks up the disk on which the provided DiskMonotask will operate, and adds it to that disk's
   * task queue.
   */
  def submitTask(task: DiskMonotask): Unit = {
    // Extract the identifier of the disk on which this task will operate.
    val diskId = task match {
      case write: DiskWriteMonotask =>
        if (loadBalanceDiskWrites) {
          // Add the monotask to all disk queues; the monotask will be run by whichever disk
          // is available sooner.
          diskIds.foreach(addToDiskQueue(task, _))
          return
        } else {
          val id = getNextDiskId()
          write.diskId = Some(id)
          id
        }

      case read: DiskReadMonotask =>
        read.diskId

      case remove: DiskRemoveMonotask =>
        remove.diskId

      case hdfsWrite: HdfsWriteMonotask =>
        hdfsWriteMonotaskQueue.put(hdfsWrite)
        return

      case hdfs: HdfsDiskMonotask =>
        hdfs.chooseLocalDir(diskIds)

      case _ =>
        val exception = new SparkException(
          s"DiskMonotask $task (id: ${task.taskId}) rejected because its subtype is not supported.")
        task.handleException(exception)
        return
    }
    addToDiskQueue(task, diskId)
  }

  private def addToDiskQueue(monotask: DiskMonotask, diskId: String): Unit = {
    if (diskIds.contains(diskId)) {
      diskAccessors(diskId).submitMonotask(monotask)
    } else {
      val exception = new SparkException(s"DiskMonotask $monotask (id: ${monotask.taskId}) has " +
        s"an invalid diskId: $diskId. Valid diskIds are: ${diskIds.mkString(", ")}")
      monotask.handleException(exception)
    }
  }

  /**
   * Returns the diskId of the disk that should be used for the next write operation. Uses a
   * round-robin disk scheduling strategy.
   */
  def getNextDiskId(): String = {
    val diskId = diskIds(nextDisk)
    nextDisk = (nextDisk + 1) % diskIds.length
    diskId
  }

  private def addShutdownHook() {
    Runtime.getRuntime.addShutdownHook(new Thread("Stop DiskAccessors") {
      override def run() = Utils.logUncaughtExceptions(DiskScheduler.this.stop())
    })
  }

  /** Triggers all DiskAccessors to stop executing, interrupting any in-progress DiskMonotasks. */
  def stop(): Unit = {
    diskAccessors.values.foreach(_.stop())
  }

  /**
   * Executes the provided monotask and catches any exceptions that it throws. Communicates to the
   * LocalDagScheduler whether the monotask completed successfully or failed.
   */
  private def executeMonotask(monotask: DiskMonotask): Unit = {
    val startTimeNanos = System.nanoTime
    try {
      monotask.execute()
      monotask.context.taskMetrics.incDiskNanos(System.nanoTime - startTimeNanos)

      logDebug(s"DiskMonotask $monotask (id: ${monotask.taskId}) operating on " +
        s"block ${monotask.blockId} completed successfully.")
      SparkEnv.get.localDagScheduler.post(TaskSuccess(monotask))
    } catch {
      case t: Throwable => {
        // An exception was thrown, causing the currently-executing DiskMonotask to fail. Attempt to
        // exit cleanly by informing the LocalDagScheduler of the failure. If this was a fatal
        // exception, we will delegate to the default uncaught exception handler, which will
        // terminate the Executor. We don't forcibly exit unless the exception was inherently fatal
        // in order to avoid stopping other tasks unnecessarily.
        if (Utils.isFatalError(t)) {
          SparkUncaughtExceptionHandler.uncaughtException(t)
        }
        if (t.isInstanceOf[ClosedByInterruptException]) {
          logDebug(s"During normal shutdown, thread '${Thread.currentThread().getName()}' was " +
            "interrupted while performing I/O.")
        }

        // Set the disk monotask time, even when a failure occurred. This cannot be done in a
        // finally clause, because the disk time needs to be set before the LocalDagScheduler is
        // notified of the task's completion.
        monotask.context.taskMetrics.incDiskNanos(System.nanoTime - startTimeNanos)
        monotask.handleException(t)
      }
    }
  }

  /**
   * Stores and services the DiskMonotask queue for the specified disk.
   *
   * @param concurrency The maximum number of DiskMonotasks that will be executed concurrently on
   *                    each disk.
   */
  private class DiskAccessor(diskId: String, concurrency: Int) extends Runnable {

    /** The name of the physical disk on which this DiskAccessor will operate. */
    val diskName = BlockFileManager.getDiskNameFromPath(diskId)

    /**
     * A queue of DiskMonotasks that are waiting to be executed. This queue first does fair
     * queueing over monotask types, essentially to maintain a good pipeline of different types
     * of monotasks to run.  Within each type of monotask, the queue round-robins over
     * the machine that the request originated from, so that requests from one machine can't
     * accumulate and temporarily starve requests from other machines.
     */
    private val taskQueue = new DeficitRoundRobinQueue[Class[_]]()

    private val numRunningAndQueuedDiskMonotasks = new AtomicInteger(0)

    private val numQueuedReadMonotasks = new AtomicInteger(0)
    private val numQueuedWriteMonotasks = new AtomicInteger(0)
    private val numQueuedRemoveMonotasks = new AtomicInteger(0)

    /**
     * An array of threads that are concurrently executing DiskMonotasks from this DiskAccessor's
     * task queue.
     */
    private val diskAccessorThreads = (1 to concurrency).map { threadIndex =>
      val diskAccessorThread = new Thread(this)
      diskAccessorThread.setDaemon(true)
      diskAccessorThread.setName(
        s"DiskAccessor thread $threadIndex of $concurrency for disk $diskId")
      diskAccessorThread.start()
      diskAccessorThread
    }

    def submitMonotask(monotask: DiskMonotask): Unit = {
      taskQueue.enqueue(monotask.getClass, monotask)
      numRunningAndQueuedDiskMonotasks.incrementAndGet()

      updateCounters(1, monotask)
    }

    /** Update the counts of queued monotasks when a monotask is added or removed from the queue. */
    private def updateCounters(change: Int, monotask: DiskMonotask): Unit = {
      monotask match {
        case _: HdfsReadMonotask =>
          numQueuedReadMonotasks.addAndGet(change)

        case _: DiskReadMonotask =>
          numQueuedReadMonotasks.addAndGet(change)

        case _: HdfsWriteMonotask =>
          numQueuedWriteMonotasks.addAndGet(change)

        case _: DiskWriteMonotask =>
          numQueuedWriteMonotasks.addAndGet(change)

        case _: DiskRemoveMonotask =>
          numQueuedRemoveMonotasks.addAndGet(change)

        case _ =>
          logWarning(s"Could  not update counters for unknown type of monotask: $monotask")
      }

    }

    // TODO: Write monotasks get put in the queue for all disks, so this
    //       includes some monotasks that are also queued on other disks. Fix this.
    def getNumRunningAndQueuedDiskMonotasks: (Int, Int, Int, Int) =
      (numRunningAndQueuedDiskMonotasks.get(),
        numQueuedReadMonotasks.get(),
        numQueuedRemoveMonotasks.get(),
        numQueuedWriteMonotasks.get())

    /** Continuously executes DiskMonotasks from the task queue. */
    def run(): Unit = {
      val currentThread = Thread.currentThread()
      val threadName = currentThread.getName()

      try {
        while (!currentThread.isInterrupted()) {
          val diskMonotask = taskQueue.dequeue()
          updateCounters(-1, diskMonotask)
          logInfo(
            s"Disk $diskName now running $diskMonotask for ${diskMonotask.context.remoteName}")
          var monotaskNotYetRun = true
          if (loadBalanceDiskWrites && diskMonotask.isInstanceOf[DiskWriteMonotask]) {
            // assignToDisk will return false if the task has already been completed by another disk
            monotaskNotYetRun = diskMonotask.asInstanceOf[DiskWriteMonotask].assignToDisk(diskId)
          }
          if (monotaskNotYetRun) {
            diskMonotask.context.taskMetrics.incDiskWaitNanos(diskMonotask.getQueueTime())
            executeMonotask(diskMonotask)
          }
          // TODO: If the monotask already ran, we should treat the queue differently: rather than
          // doing round-robin, we should select from the same queue again!
          numRunningAndQueuedDiskMonotasks.decrementAndGet()
        }
      } catch {
        case _: InterruptedException => {
          // An InterruptedException was thrown during the call to taskQueue.take(), above. Since
          // throwing an InterruptedException clears a thread's interrupt status, it is proper form
          // to reset the interrupt status by calling Thread.currentThread().interrupt() after
          // catching an InterruptedException so that other code knows that this thread has been
          // interrupted.
          //
          // This try/catch block is not strictly necessary, since we want the run() method to end
          // when an InterruptedException is thrown. It is included so that the DiskAccessor stops
          // cleanly (and does not throw a benign InterruptedException that is visible to the user).
          logDebug(s"Thread '$threadName' interrupted while waiting for more DiskMonotasks.")
          currentThread.interrupt()
        }
      }
    }

    /**
     * Interrupts all of this DiskAccessor's threads, causing them to stop executing DiskMonotasks
     * and expire. During normal operation, this method will be called once all DiskMonotasks have
     * been executed and the DiskScheduler is ready to shut down. In that case, all of the threads
     * will be interrupted while waiting for more tasks to be added to the task queue. This is
     * harmless. However, if any threads are still executing tasks, this will interrupt them before
     * they finish.
     */
    def stop(): Unit = {
      diskAccessorThreads.foreach { diskAccessorThread =>
        logDebug(s"Attempting to stop thread: ${diskAccessorThread.getName()}")
        diskAccessorThread.interrupt()
      }
    }
  }
}

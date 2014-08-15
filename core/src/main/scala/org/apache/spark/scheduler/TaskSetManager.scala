/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.io.NotSerializableException
import java.util.Arrays

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.math.max
import scala.math.min

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.{Clock, SystemClock}
import scala.collection.mutable

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails more than this number of times, the entire
 *                        task set will be aborted
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,
    val maxTaskFailures: Int,
    clock: Clock = SystemClock)
  extends Schedulable with Logging {

  val conf = sched.sc.conf

  /*
   * Sometimes if an executor is dead or in an otherwise invalid state, the driver
   * does not realize right away leading to repeated task failures. If enabled,
   * this temporarily prevents a task from re-launching on an executor where
   * it just failed.
   */
  private val EXECUTOR_TASK_BLACKLIST_TIMEOUT =
    conf.getLong("spark.scheduler.executorTaskBlacklistTime", 0L)

  // Quantile of tasks at which to start speculation
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75)
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5)

  // Serializer for closures and tasks.
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks
  val indexOf: Map[Task[_], Int] = tasks.zipWithIndex.toMap
  val miniStageByTask = taskSet.miniStageByTask

  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)
  val successful = new HashMap[Int, TaskInfo]()
  private val numFailures = new Array[Int](numTasks)
  // key is taskId, value is a Map of executor id to when it failed
  private val failedExecutors = new HashMap[Int, HashMap[String, Long]]()
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  var tasksSuccessful = 0

  var weight = 1
  var minShare = 0
  var priority = taskSet.priority
  var stageId = taskSet.stageId
  var name = "TaskSet_" + taskSet.stageId.toString
  var parent: Pool = null

  val runningTasksSet = new HashSet[Long]
  override def runningTasks = runningTasksSet.size

  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  var isZombie = false

  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. They are also only cleaned up lazily;
  // when a task is launched, it remains in all the pending lists except
  // the one that it was launched from, but gets removed from them later.
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack -- similar to the above.
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences.
  var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // Set containing all pending tasks (also used as a stack, as above).
  val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  val taskInfos = new HashMap[Long, TaskInfo]

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  // Figure out which locality levels we have in our TaskSet, so we can do delay scheduling
  var myLocalityLevels = computeValidLocalityLevels()
  var localityWaits = myLocalityLevels.map(getLocalityWait) // Time to wait at each level

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  var currentLocalityIndex = 0    // Index of our current locality level in validLocalityLevels
  var lastLaunchTime = clock.getTime()  // Time we last launched a task at this level

  override def schedulableQueue = null

  override def schedulingMode = SchedulingMode.NONE

  var emittedTaskSizeWarning = false

  /**
   * Add a task to all the pending-task lists that it should be on. If readding is set, we are
   * re-adding the task so only include it in each list if it's not already there.
   */
  private def addPendingTask(index: Int, readding: Boolean = false) {
    // Utility method that adds `index` to a list only if readding=false or it's not already there
    def addTo(list: ArrayBuffer[Int]) {
      if (!readding || !list.contains(index)) {
        list += index
      }
    }

    var hadAliveLocations = false
    for (loc <- tasks(index).preferredLocations) {
      for (execId <- loc.executorId) {
        addTo(pendingTasksForExecutor.getOrElseUpdate(execId, new ArrayBuffer))
      }
      if (sched.hasExecutorsAliveOnHost(loc.host)) {
        hadAliveLocations = true
      }
      addTo(pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer))
      for (rack <- sched.getRackForHost(loc.host)) {
        addTo(pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer))
        if(sched.hasHostAliveOnRack(rack)){
          hadAliveLocations = true
        }
      }
    }

    if (!hadAliveLocations) {
      // Even though the task might've had preferred locations, all of those hosts or executors
      // are dead; put it in the no-prefs list so we can schedule it elsewhere right away.
      addTo(pendingTasksWithNoPrefs)
    }

    if (!readding) {
      allPendingTasks += index  // No point scanning this whole list to find the old task there
    }
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   */
  private def findTaskFromList(execId: String, list: ArrayBuffer[Int]): Option[Int] = {
    var indexOffset = list.size

    while (indexOffset > 0) {
      indexOffset -= 1
      val index = list(indexOffset)
      if (!executorIsBlacklisted(execId, index)) {
        // This should almost always be list.trimEnd(1) to remove tail
        list.remove(indexOffset)
        if (copiesRunning(index) == 0 && !successful.contains(index)) {
          return Some(index)
        }
      }
    }
    None
  }

  /** Check whether a task is currently running an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  /**
   * Is this re-execution of a failed task on an executor it already failed in before
   * EXECUTOR_TASK_BLACKLIST_TIMEOUT has elapsed ?
   */
  private def executorIsBlacklisted(execId: String, taskId: Int): Boolean = {
    if (failedExecutors.contains(taskId)) {
      val failed = failedExecutors.get(taskId).get

      return failed.contains(execId) &&
        clock.getTime() - failed.get(execId).get < EXECUTOR_TASK_BLACKLIST_TIMEOUT
    }

    false
  }

  /**
   * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   */
  private def findSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    speculatableTasks.retain(index => !successful.contains(index)) // Remove finished tasks from set

    def canRunOnHost(index: Int): Boolean =
      !hasAttemptOnHost(index, host) && !executorIsBlacklisted(execId, index)

    if (!speculatableTasks.isEmpty) {
      // Check for process-local or preference-less tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      for (index <- speculatableTasks if canRunOnHost(index)) {
        val prefs = tasks(index).preferredLocations
        val executors = prefs.flatMap(_.executorId)
        if (prefs.size == 0 || executors.contains(execId)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations.map(_.host)
          if (locations.contains(host)) {
            speculatableTasks -= index
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        for (rack <- sched.getRackForHost(host)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val racks = tasks(index).preferredLocations.map(_.host).map(sched.getRackForHost)
            if (racks.contains(rack)) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.ANY))
        }
      }
    }

    None
  }

  /** What resources does the task require? */
  def claimedResources(taskId: Long): Resources = {
    miniStageByIndex(taskInfos(taskId).index).resourceRequirements
  }

  /** Return all pipeline tasks that are ready to run for a given WorkerOffer */
  private def readyTasksIndices(offer: WorkerOffer): Seq[Int] = {
    (0 until tasks.length).filter(canRun(_, offer))
  }

  /** Can a task run on a given offer? */
  private def canRun(taskIndex: Int, offer: WorkerOffer): Boolean = {
    // TODO(ryan): the foreaches are slow (especially with many tasks),
    // and there should be a (likely painful) way to forgo a lot of the computation
    val sufficientResources = offer.resources.canFulfill(miniStageByIndex(taskIndex).resourceRequirements)

    lazy val satisfiedDependencies = dependenciesByIndex(taskIndex).forall {
      index => successful.contains(index) && successful(index).host == offer.host
    }
    val cotasks = cotasksByIndex(taskIndex)
    assert(allSameMachine(cotasks))
    lazy val onSameMachineAsCotasks = cotasks.forall {
      index => !successful.contains(index) || successful(index).host == offer.host
    }
    sufficientResources && satisfiedDependencies && onSameMachineAsCotasks
  }

  private def allSameMachine(taskIndices: Seq[Int]): Boolean = {
    val hosts = taskIndices.flatMap(successful.get(_).map(_.host))
    hosts.toSet.size < 2 // there is either 0 or 1 unique host(s)
  }

  // TODO(ryan): below methods can be precomputed into a Map
  private def miniStageByIndex(index: Int): MiniStage = {
    miniStageByTask(tasks(index))
  }

  private def cotasksByIndex(index: Int): Seq[Int] = {
    val task = tasks(index)
    miniStageByTask(task).cotasks(task).map(indexOf)
  }

  private def dependenciesByIndex(index: Int): Seq[Int] = {
    val task = tasks(index)
    val dependentStages: Seq[MiniStage] = miniStageByTask(task).dependencies
    dependentStages.flatMap(_.dependenciesOfChild(task)).map(indexOf)
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   *
   * @return An option containing (task index within the task set, locality, is speculative?)
   */
  private def findTask(offer: WorkerOffer, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
    val indices = readyTasksIndices(offer).filter(index => copiesRunning(index) == 0 && !successful.contains(index))
    if (indices.length == 0) {
      None
    } else {
      Some(indices.head, TaskLocality.PROCESS_LOCAL, false)
    }

    /*
    for (index <- findTaskFromList(execId, getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
      for (index <- findTaskFromList(execId, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- findTaskFromList(execId, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }

    // Look for no-pref tasks after rack-local tasks since they can run anywhere.
    for (index <- findTaskFromList(execId, pendingTasksWithNoPrefs)) {
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
      for (index <- findTaskFromList(execId, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // Finally, if all else has failed, find a speculative task
    findSpeculativeTask(execId, host, locality).map { case (taskIndex, allowedLocality) =>
      (taskIndex, allowedLocality, true)
    }
    */
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   */
  def resourceOffer(
      offer: WorkerOffer,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    val execId = offer.executorId
    val host = offer.host
    if (!isZombie) {
      val curTime = clock.getTime()

      var allowedLocality = getAllowedLocalityLevel(curTime)
      if (allowedLocality > maxLocality) {
        allowedLocality = maxLocality   // We're not allowed to search for farther-away tasks
      }

      findTask(offer, allowedLocality) match {
        case Some((index, taskLocality, speculative)) => {
          // Found a task; do some bookkeeping and return a task description
          val task = tasks(index)
          val taskId = sched.newTaskId()
          // Do various bookkeeping
          copiesRunning(index) += 1
          val attemptNum = taskAttempts(index).size
          val info = new TaskInfo(taskId, index, attemptNum, curTime,
            execId, host, taskLocality, speculative)
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)
          // Update our locality level for delay scheduling
          currentLocalityIndex = getLocalityIndex(taskLocality)
          lastLaunchTime = curTime
          // Serialize and return the task
          val startTime = clock.getTime()
          // We rely on the DAGScheduler to catch non-serializable closures and RDDs, so in here
          // we assume the task can be serialized without exceptions.
          val serializedTask = Task.serializeWithDependencies(
            task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
              !emittedTaskSizeWarning) {
            emittedTaskSizeWarning = true
            logWarning(s"Stage ${task.stageId} contains a task of very large size " +
              s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
              s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
          }
          addRunningTask(taskId)

          // We used to log the time it takes to serialize the task, but task size is already
          // a good proxy to task serialization time.
          // val timeTaken = clock.getTime() - startTime
          val taskName = s"task ${info.id} in stage ${taskSet.id}"
          logInfo("Starting %s (TID %d, %s, %s, %d bytes)".format(
              taskName, taskId, host, taskLocality, serializedTask.limit))

          sched.dagScheduler.taskStarted(task, info)
          return Some(new TaskDescription(taskId, execId, taskName, index, serializedTask))
        }
        case _ =>
      }
    }
    None
  }

  private def maybeFinishTaskSet() {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
    }
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    while (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex) &&
        currentLocalityIndex < myLocalityLevels.length - 1)
    {
      // Jump to the next locality level, and remove our waiting time for the current one since
      // we don't want to count it again on the next one
      lastLaunchTime += localityWaits(currentLocalityIndex)
      currentLocalityIndex += 1
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  def handleTaskGettingResult(tid: Long) = {
    val info = taskInfos(tid)
    info.markGettingResult()
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Marks the task as successful and notifies the DAGScheduler that a task has ended.
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]) = {
    val info = taskInfos(tid)
    val index = info.index
    info.markSuccessful()
    removeRunningTask(tid)
    sched.dagScheduler.taskEnded(
      tasks(index), Success, result.value(), result.accumUpdates, info, result.metrics)
    if (copiesRunning(index) > 0) {
      copiesRunning(index) -= 1
    } else {
      logWarning("Task finished but no copies of it were running?!")
    }
    if (!successful.contains(index)) {
      tasksSuccessful += 1
      logInfo("Finished task %s in stage %s (TID %d) in %d ms on %s (%d/%d)".format(
        info.id, taskSet.id, info.taskId, info.duration, info.host, tasksSuccessful, numTasks))
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = info
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    failedExecutors.remove(index)
    maybeFinishTaskSet()
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskEndReason) {
    val info = taskInfos(tid)
    if (info.failed) {
      return
    }
    removeRunningTask(tid)
    info.markFailed()
    val index = info.index
    copiesRunning(index) -= 1
    var taskMetrics : TaskMetrics = null

    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}): " +
      reason.asInstanceOf[TaskFailedReason].toErrorString
    reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful.contains(index)) {
          successful(index) = info // TODO(ryan) why are we marking it as successful??
          tasksSuccessful += 1
        }
        // Not adding to failed executors for FetchFailed.
        isZombie = true

      case ef: ExceptionFailure =>
        taskMetrics = ef.metrics.orNull
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTime()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid) on executor ${info.host}: " +
            s"${ef.className} (${ef.description}) [duplicate $dupCount]")
        }

      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)

      case e: TaskEndReason =>
        logError("Unknown TaskEndReason: " + e)
    }
    // always add to failed executors
    failedExecutors.getOrElseUpdate(index, new HashMap[String, Long]()).
      put(info.executorId, clock.getTime())
    sched.dagScheduler.taskEnded(tasks(index), reason, null, null, info, taskMetrics)
    addPendingTask(index)
    if (!isZombie && state != TaskState.KILLED) {
      assert (null != failureReason)
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason))
        return
      }
    }
    maybeFinishTaskSet()
  }

  def abort(message: String) {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
   */
  def addRunningTask(tid: Long) {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it. */
  def removeRunningTask(tid: Long) {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks */
  override def executorLost(execId: String, host: String) {
    logInfo("Re-queueing tasks for " + execId + " from TaskSet " + taskSet.id)

    // Re-enqueue pending tasks for this host based on the status of the cluster -- for example, a
    // task that used to have locations on only this host might now go to the no-prefs list. Note
    // that it's okay if we add a task to the same queue twice (if it had multiple preferred
    // locations), because findTaskFromList will skip already-running tasks.
    for (index <- getPendingTasksForExecutor(execId)) {
      addPendingTask(index, readding=true)
    }
    for (index <- getPendingTasksForHost(host)) {
      addPendingTask(index, readding=true)
    }

    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (tasks(0).isInstanceOf[ShuffleMapTask]) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        if (successful.contains(index)) {
          successful -= index
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(tasks(index), Resubmitted, null, null, info, null)
        }
      }
    }
    // Also re-enqueue any tasks that were running on the node
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure)
    }
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   * TODO: To make this scale to large jobs, we need to maintain a list of running tasks, so that
   * we don't scan the whole task set. It might also help to make this sorted by launch time.
   */
  override def checkSpeculatableTasks(): Boolean = {
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.
    if (isZombie || numTasks == 1) {
      return false
    }
    var foundTasks = false
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) {
      val time = clock.getTime()
      val durations = taskInfos.values.filter(_.successful).map(_.duration).toArray
      Arrays.sort(durations)
      val medianDuration = durations(min((0.5 * tasksSuccessful).round.toInt, durations.size - 1))
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, 100)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for ((tid, info) <- taskInfos) {
        val index = info.index
        if (!successful.contains(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          speculatableTasks += index
          foundTasks = true
        }
      }
    }
    foundTasks
  }

  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    val defaultWait = conf.get("spark.locality.wait", "3000")
    level match {
      case TaskLocality.PROCESS_LOCAL =>
        conf.get("spark.locality.wait.process", defaultWait).toLong
      case TaskLocality.NODE_LOCAL =>
        conf.get("spark.locality.wait.node", defaultWait).toLong
      case TaskLocality.RACK_LOCAL =>
        conf.get("spark.locality.wait.rack", defaultWait).toLong
      case TaskLocality.ANY =>
        0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (!pendingTasksForExecutor.isEmpty && getLocalityWait(PROCESS_LOCAL) != 0 &&
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty && getLocalityWait(NODE_LOCAL) != 0 &&
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    if (!pendingTasksForRack.isEmpty && getLocalityWait(RACK_LOCAL) != 0 &&
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  // Re-compute pendingTasksWithNoPrefs since new preferred locations may become available
  def executorAdded() {
    def newLocAvail(index: Int): Boolean = {
      for (loc <- tasks(index).preferredLocations) {
        if (sched.hasExecutorsAliveOnHost(loc.host) ||
           (sched.getRackForHost(loc.host).isDefined &&
           sched.hasHostAliveOnRack(sched.getRackForHost(loc.host).get))) {
          return true
        }
      }
      false
    }
    logInfo("Re-computing pending task lists.")
    pendingTasksWithNoPrefs = pendingTasksWithNoPrefs.filter(!newLocAvail(_))
    myLocalityLevels = computeValidLocalityLevels()
    localityWaits = myLocalityLevels.map(getLocalityWait)
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KB = 100
}

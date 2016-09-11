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

package org.apache.spark.scheduler

import java.util.Properties

import scala.collection.mutable.HashMap

import org.mockito.Mockito.mock

import org.scalatest.FunSuite

import org.apache.spark._

class FakeSchedulerBackend extends SchedulerBackend {
  def start() {}
  def stop() {}
  def reviveOffers() {}
  def defaultParallelism() = 1
}

class TaskSchedulerImplSuite extends FunSuite with LocalSparkContext with Logging {

  test("Scheduler does not always schedule tasks on the same workers") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    val numFreeCores = 1
    val workerOffers = Seq(
      new WorkerOffer(
        "executor0", "host0", numFreeCores, totalDisks = 0, HashMap.empty[String, Int]),
      new WorkerOffer(
        "executor1", "host1", numFreeCores, totalDisks = 0, HashMap.empty[String, Int]))
    // Repeatedly try to schedule a 1-task job, and make sure that it doesn't always
    // get scheduled on the same executor. While there is a chance this test will fail
    // because the task randomly gets placed on the first executor all 1000 times, the
    // probability of that happening is 2^-1000 (so sufficiently small to be considered
    // negligible).
    val numTrials = 1000
    val selectedExecutorIds = 1.to(numTrials).map { _ =>
      val taskSet = FakeTask.createTaskSet(1)
      taskScheduler.submitTasks(taskSet)
      val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
      assert(1 === taskDescriptions.length)
      taskDescriptions(0).executorId
    }
    val count = selectedExecutorIds.count(_ == workerOffers(0).executorId)
    assert(count > 0)
    assert(count < numTrials)
  }

  test("Scheduler correctly accounts for multiple CPUs per task") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskCpus = 2

    sc.conf.set("spark.task.cpus", taskCpus.toString)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    // Give zero core offers. Should not generate any tasks
    val zeroCoreWorkerOffers = Seq(
      new WorkerOffer("executor0", "host0", freeSlots = 0, totalDisks = 0, HashMap.empty[String, Int]),
      new WorkerOffer("executor1", "host1", freeSlots = 0, totalDisks = 0, HashMap.empty[String, Int]))
    val taskSet = FakeTask.createTaskSet(1)
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(zeroCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // No tasks should run as we only have 1 core free.
    val numFreeCores = 1
    val singleCoreWorkerOffers = Seq(
      new WorkerOffer("executor0", "host0", numFreeCores, totalDisks = 0, HashMap.empty[String, Int]),
      new WorkerOffer("executor1", "host1", numFreeCores, totalDisks = 0, HashMap.empty[String, Int]))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(singleCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now change the offers to have 2 cores in one executor and verify if it
    // is chosen.
    val multiCoreWorkerOffers = Seq(
      new WorkerOffer("executor0", "host0", taskCpus, totalDisks = 0, HashMap.empty[String, Int]),
      new WorkerOffer("executor1", "host1", numFreeCores, totalDisks = 0, HashMap.empty[String, Int]))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
  }

  test("Scheduler does not crash when tasks are not serializable") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskCpus = 2

    sc.conf.set("spark.task.cpus", taskCpus.toString)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    val numFreeCores = 1
    var taskSet = new TaskSet(Array(
      new NotSerializableFakeTask(1, 0),
      new NotSerializableFakeTask(0, 1)), 0, 0, 0, null)
    val multiCoreWorkerOffers = Seq(
      new WorkerOffer("executor0", "host0", taskCpus, totalDisks = 0, HashMap.empty[String, Int]),
      new WorkerOffer(
        "executor1", "host1", numFreeCores, totalDisks = 0, HashMap.empty[String, Int]))
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now check that we can still submit tasks
    // Even if one of the tasks has not-serializable tasks, the other task set should still be
    // processed without error
    taskScheduler.submitTasks(taskSet)
    taskScheduler.submitTasks(FakeTask.createTaskSet(1))
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(taskDescriptions.map(_.executorId) === Seq("executor0"))
  }

  test("Scheduler ignores reduce locality constraints when no local locations are available") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    val taskSet = FakeTask.createTaskSet(3,
      Seq(ReduceExecutorTaskLocation("host0", "executor0")),
      Seq(ReduceExecutorTaskLocation("host0", "executor0")),
      Seq(ReduceExecutorTaskLocation("host1", "executor1")))

    val host0WorkerOffer = Seq(new WorkerOffer(
      "executor0", "host0", freeSlots = 1, totalDisks = 0, HashMap.empty[String, Int]))
    val host1WorkerOffer = Seq(new WorkerOffer(
      "executor1", "host1", freeSlots = 1, totalDisks = 0, HashMap.empty[String, Int]))

    taskScheduler.submitTasks(taskSet)

    // When host0 is offered, the first task should be scheduled on it (since that's the first task
    // with a locality preference for host0).
    var taskDescriptions = taskScheduler.resourceOffers(host0WorkerOffer).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
    assert(0 === taskDescriptions(0).index)

    // When host1 is offered, the last task should be scheduled on it, since that's the only task
    // with a locality preference for host1.
    taskDescriptions = taskScheduler.resourceOffers(host1WorkerOffer).flatten
    assert(1 === taskDescriptions.length)
    assert("executor1" === taskDescriptions(0).executorId)
    assert(2 === taskDescriptions(0).index)

    // When host1 is offered again, there are no tasks with locality preferences for that machine,
    // so eventually the last remaining task should get assigned to it.
    taskDescriptions = taskScheduler.resourceOffers(host1WorkerOffer).flatten
    assert(1 === taskDescriptions.length)
    assert("executor1" === taskDescriptions(0).executorId)
    assert(1 === taskDescriptions(0).index)
  }

  test("Scheduler offers resources to task set with fewest tasks running on machine first") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    val taskSet1 = FakeTask.createTaskSet(3)

    val host0WorkerOffer = Seq(new WorkerOffer(
      "executor0", "host0", freeSlots = 1, totalDisks = 0, HashMap.empty[String, Int]))

    taskScheduler.submitTasks(taskSet1)

    // At this point, there's only one outstanding task set, so the offer should be accepted by
    // that task set.
    var taskDescriptions = taskScheduler.resourceOffers(host0WorkerOffer).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
    assert(taskSet1.id === taskScheduler.taskIdToTaskSetId(taskDescriptions(0).taskId))

    // When another offer is made on the same machine, it should again be accepted by taskSet1,
    // because there are no other task sets.
    host0WorkerOffer(0).taskSetIdToRunningTasks(taskSet1.id) = 1
    taskDescriptions = taskScheduler.resourceOffers(host0WorkerOffer).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
    assert(taskSet1.id === taskScheduler.taskIdToTaskSetId(taskDescriptions(0).taskId))

    val taskSet2 = FakeTask.createTaskSet(2)
    taskScheduler.submitTasks(taskSet2)

    // Another offer on worker0 should go to taskSet2, because it doesn't have any tasks running
    // on the machine.
    host0WorkerOffer(0).taskSetIdToRunningTasks(taskSet1.id) = 2
    taskDescriptions = taskScheduler.resourceOffers(host0WorkerOffer).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
    assert(taskSet2.id === taskScheduler.taskIdToTaskSetId(taskDescriptions(0).taskId))

    // Now offer a spot on worker 1.  Since neither task set has anything running there, it
    // should go to the task set that was submitted first.
    val host1WorkerOffer = Seq(new WorkerOffer(
      "executor1", "host1", freeSlots = 1, totalDisks = 0, HashMap.empty[String, Int]))
    taskDescriptions = taskScheduler.resourceOffers(host1WorkerOffer).flatten
    assert(1 === taskDescriptions.length)
    assert("executor1" === taskDescriptions(0).executorId)
    assert(taskSet1.id === taskScheduler.taskIdToTaskSetId(taskDescriptions(0).taskId))
  }

  test("Scheduler assigns correct number of tasks based on task set's resource requirements " +
    "when using slot-based monotasks scheduler") {
    val conf = new SparkConf(false)
    conf.set("spark.monotasks.scheduler", "slot")
    sc = new SparkContext("local", "TaskSchedulerImplSuite", conf)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    // Offer one worker that has 5 free slots and 3 total disks to task sets with
    // different resource requirements.
    val workerOffers = Seq(new WorkerOffer(
      "executor0", "host0", freeSlots = 5, totalDisks = 3, HashMap.empty[String, Int]))

    def verifyNumberOfOffersAccepted(
        numTasksInTaskSet: Int,
        usesDisk: Boolean,
        usesNetwork: Boolean,
        numExpectedAcceptedOffers: Int): Unit = {
      val tasks = Array.tabulate[Macrotask[_]](numTasksInTaskSet)(new FakeTask(_, Nil))
      taskScheduler.submitTasks(new TaskSet(tasks, 0, 0, 0, null, usesDisk, usesNetwork))
      val tasksScheduled = taskScheduler.resourceOffers(workerOffers).flatten
      assert(numExpectedAcceptedOffers === tasksScheduled.length)

      // Keep offering resources until all of the TaskSet's tasks have been assigned. We do this so
      // that there aren't any tasks from the given TaskSet left in the scheduling queue when we
      // submit the next task set.
      while (taskScheduler.resourceOffers(workerOffers).flatten.length > 0) {
        // Do nothing.
      }
    }

    // For a task set that only uses the CPU, only 1 task should be assigned (because the disk and
    // network slots can't be used).
    verifyNumberOfOffersAccepted(
      numTasksInTaskSet = 10,
      usesDisk = false,
      usesNetwork = false,
      numExpectedAcceptedOffers = 1)

    // For a task set that uses the CPU and disk, 4 tasks should be assigned (one for each slot
    // except the network slot).
    verifyNumberOfOffersAccepted(
      numTasksInTaskSet = 10,
      usesDisk = true,
      usesNetwork = false,
      numExpectedAcceptedOffers = 4)

    // For a task set that uses the CPU and network, 2 tasks should be assigned.
    verifyNumberOfOffersAccepted(
      numTasksInTaskSet = 10,
      usesDisk = false,
      usesNetwork = true,
      numExpectedAcceptedOffers = 2)

    // For a task set that uses all three resources, all 5 slots should be used.
    verifyNumberOfOffersAccepted(
      numTasksInTaskSet = 10,
      usesDisk = true,
      usesNetwork = true,
      numExpectedAcceptedOffers = 5)
  }
}

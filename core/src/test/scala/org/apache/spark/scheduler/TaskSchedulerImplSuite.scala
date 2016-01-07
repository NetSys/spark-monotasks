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

  /**
   * Removes all jobs from the scheduler. Necessary because monotasks throws an error if more
   * than one task set is outstanding.
   */
  def clearOutstandingJobs(taskScheduler: TaskSchedulerImpl) {
    // Remove the task set from the scheduler, since Monotasks currently throws an error if more
    // than one task set is currently outstanding.
    val rootPool = taskScheduler.rootPool
    rootPool.getSortedTaskSetQueue.foreach(rootPool.removeSchedulable(_))
  }

  test("Scheduler does not always schedule tasks on the same workers") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    val numFreeCores = 1
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores, totalDisks = 0),
      new WorkerOffer("executor1", "host1", numFreeCores, totalDisks = 0))
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
      clearOutstandingJobs(taskScheduler)
      taskDescriptions(0).executorId
    }
    val count = selectedExecutorIds.count(_ == workerOffers(0).executorId)
    assert(count > 0)
    assert(count < numTrials)
  }

  // This test does not currently work because monotasks re-writes the worker
  // offers.
  ignore("Scheduler correctly accounts for multiple CPUs per task") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskCpus = 2

    sc.conf.set("spark.task.cpus", taskCpus.toString)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    // Give zero core offers. Should not generate any tasks
    val zeroCoreWorkerOffers = Seq(
      new WorkerOffer("executor0", "host0", freeSlots = 0, totalDisks = 0),
      new WorkerOffer("executor1", "host1", freeSlots = 0, totalDisks = 0))
    val taskSet = FakeTask.createTaskSet(1)
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(zeroCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)
    clearOutstandingJobs(taskScheduler)

    // No tasks should run as we only have 1 core free.
    val numFreeCores = 1
    val singleCoreWorkerOffers = Seq(
      new WorkerOffer("executor0", "host0", numFreeCores, totalDisks = 0),
      new WorkerOffer("executor1", "host1", numFreeCores, totalDisks = 0))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(singleCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)
    clearOutstandingJobs(taskScheduler)

    // Now change the offers to have 2 cores in one executor and verify if it
    // is chosen.
    val multiCoreWorkerOffers = Seq(
      new WorkerOffer("executor0", "host0", taskCpus, totalDisks = 0),
      new WorkerOffer("executor1", "host1", numFreeCores, totalDisks = 0))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
    clearOutstandingJobs(taskScheduler)
  }

  // This test does not currently work because monotasks re-writes the worker
  // offers.
  ignore("Scheduler does not crash when tasks are not serializable") {
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
      new WorkerOffer("executor0", "host0", taskCpus, totalDisks = 0),
      new WorkerOffer("executor1", "host1", numFreeCores, totalDisks = 0))
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now check that we can still submit tasks
    // Even if one of the task sets has not-serializable tasks, the other task set should still be
    // processed without error
    taskScheduler.submitTasks(taskSet)
    clearOutstandingJobs(taskScheduler)
    taskScheduler.submitTasks(FakeTask.createTaskSet(1))
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(taskDescriptions.map(_.executorId) === Seq("executor0"))
  }

  test("Scheduler assigns correct number of tasks based on task set's resource requirements " +
      "when using slot-based monotasks scheduler") {
    val conf = new SparkConf(false)
    conf.set("spark.monotasks.scheduler", "slot")
    sc = new SparkContext("local", "TaskSchedulerImplSuite", conf)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    // Offer one worker that has 1 free (CPU) slot and 3 total disks to task sets with
    // different resource requirements.
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", freeSlots = 1, totalDisks = 3))

    def verifyNumberOfOffersAccepted(
        numTasksInTaskSet: Int,
        usesDisk: Boolean,
        usesNetwork: Boolean,
        numExpectedAcceptedOffers: Int): Unit = {
      val tasks = Array.tabulate[Macrotask[_]](numTasksInTaskSet)(new FakeTask(_, Nil))
      taskScheduler.submitTasks(new TaskSet(tasks, 0, 0, 0, null, usesDisk, usesNetwork))
      val tasksScheduled = taskScheduler.resourceOffers(workerOffers).flatten
      assert(numExpectedAcceptedOffers === tasksScheduled.length)

      clearOutstandingJobs(taskScheduler)
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

  test("Scheduler assigns correct number of tasks when using balanced monotasks scheduler") {
    val conf = new SparkConf(false)
    conf.set("spark.monotasks.scheduler", "balanced")
    sc = new SparkContext("local", "TaskSchedulerImplSuite", conf)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    taskScheduler.setDAGScheduler(mock(classOf[DAGScheduler]))

    // Offer one worker that has 2 free slots and 3 total disks to task sets with
    // different numbers of tasks.
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", freeSlots = 2, totalDisks = 3),
      new WorkerOffer("executor1", "host1", freeSlots = 2, totalDisks = 3))

    val numTasksInTaskSet = 21
    val tasks = Array.tabulate[Macrotask[_]](numTasksInTaskSet)(new FakeTask(_, Nil))
    taskScheduler.submitTasks(
      new TaskSet(tasks, 0, 0, 0, null, usesDisk = false, usesNetwork = false))
    val tasksScheduled = taskScheduler.resourceOffers(workerOffers)
    assert(2 === tasksScheduled.length, "Expect tasks to have been assigned to both executors")
    assert(numTasksInTaskSet === tasksScheduled.flatten.length,
      "Expect all 21 tasks to have been assigned to a worker")
    tasksScheduled.foreach { tasksOnOneMachine =>
      // 10 or 11 tasks should have been assigned to each worker.
      assert(tasksOnOneMachine.length >= 10)
      assert(tasksOnOneMachine.length <= 11)
    }
  }
}

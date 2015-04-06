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

import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{Dependency, LocalSparkContext, SparkConf, SparkContext, SparkEnv,
  TaskContextImpl}
import org.apache.spark.monotasks.{LocalDagScheduler, Monotask}
import org.apache.spark.monotasks.compute.{ExecutionMonotask, ResultMonotask,
  ResultSerializationMonotask}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class MacrotaskSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  before {
    sc = new SparkContext("local", "test", new SparkConf(false))
  }

  test("buildDag(): the resulting DAG has exactly one sink (a ResultSerializationMonotask)") {
    // The DAG should look like this:
    //
    //                                 ,-- Serialization -- DiskWrite --,
    //               ,-- RddCompute --<                                  \
    // RddCompute --<                  `-- Result ------------------------>-- ResultSerialization
    //               \                                                   /
    //                `-- Serialization -- DiskWrite -------------------'
    //
    val localDagScheduler = mock(classOf[LocalDagScheduler])
    when(localDagScheduler.blockManager).thenReturn(SparkEnv.get.blockManager)
    val context = mock(classOf[TaskContextImpl])
    when(context.localDagScheduler).thenReturn(localDagScheduler)

    val diskLevel = StorageLevel.DISK_ONLY
    val rdd = sc.parallelize(1 to 10).persist(diskLevel).map(2 * _).persist(diskLevel)
    val partition = rdd.partitions.head
    val dependencyIdToPartitions = Dependency.getDependencyIdToPartitions(rdd, partition.index)
    val nextMonotask = new ResultMonotask[Int, Int](context, rdd, partition, ((a, b) => 1))

    val monotasks = new Macrotask(1, partition, dependencyIdToPartitions) {
      override def getExecutionMonotask(
          context: TaskContextImpl): (RDD[_], ExecutionMonotask[_,_]) = (rdd, nextMonotask)
    }.getMonotasks(context)

    val sinks = monotasks.filter(_.dependents.isEmpty)
    assert(sinks.size === 1)
    val sink = sinks.head
    assert(sink.isInstanceOf[ResultSerializationMonotask])
    assert(sink.dependencies.size === 3)
  }
}

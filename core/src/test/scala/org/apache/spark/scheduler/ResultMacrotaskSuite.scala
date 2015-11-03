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

import java.nio.ByteBuffer

import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{Dependency, LocalSparkContext, SparkConf, SparkContext, SparkEnv,
  TaskContext, TaskContextImpl}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.DependencyManager
import org.apache.spark.monotasks.compute.ResultSerializationMonotask
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{Serializer, SerializerInstance}
import org.apache.spark.storage.StorageLevel

class ResultMacrotaskSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  before {
    sc = new SparkContext("local", "test", new SparkConf(false))
  }

  test("getMonotasks() correctly constructs the monotasks DAG") {
    // The DAG should look like this:
    //
    //                                 ,-- Serialization -- DiskWrite --,
    //               ,-- RddCompute --<                                  \
    // RddCompute --<                  `-- Result ------------------------>-- ResultSerialization
    //               \                                                   /
    //                `-- Serialization -- DiskWrite -------------------'
    //
    val context = new TaskContextImpl(0, 0)
    val diskLevel = StorageLevel.DISK_ONLY
    val rdd = sc.parallelize(1 to 10).persist(diskLevel).map(2 * _).persist(diskLevel)

    // Setup a SparkEnv that has a serializer that will return the above RDD and a dummy function
    // when deserialize() is called.
    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.blockManager).thenReturn(SparkEnv.get.blockManager)
    SparkEnv.set(sparkEnv)
    val serializer = mock(classOf[Serializer])
    val serializerInstance = mock(classOf[SerializerInstance])
    when(sparkEnv.closureSerializer).thenReturn(serializer)
    when(serializer.newInstance()).thenReturn(serializerInstance)
    val dependencyManager = mock(classOf[DependencyManager])
    when(sparkEnv.dependencyManager).thenReturn(dependencyManager)
    val dummyFunction = ((c: TaskContext, it: Iterator[Int]) => 1)
    val dummyTaskBinary = Array.fill[Byte](5)(1)
    when(serializerInstance.deserialize[(RDD[Int], (TaskContext, Iterator[Int]) => Int)](
      ByteBuffer.wrap(dummyTaskBinary), null)).thenReturn((rdd, dummyFunction))

    // Setup the constructor parameters for ResultMacrotask.
    val partition = rdd.partitions.head
    val dependencyIdToPartitions = Dependency.getDependencyIdToPartitions(rdd, partition.index)
    val broadcastBinary = mock(classOf[Broadcast[Array[Byte]]])
    when(broadcastBinary.value).thenReturn(dummyTaskBinary)

    val macrotask =
      new ResultMacrotask(0, broadcastBinary, partition, dependencyIdToPartitions, Nil, 0)
    val monotasks = macrotask.getMonotasks(context)

    assert(monotasks.size === 8)

    // Ensure there is exactly one sink (the ResultSerializationMonotask).
    val sinks = monotasks.filter(_.dependents.isEmpty)
    assert(sinks.size === 1)
    val sink = sinks.head
    assert(sink.isInstanceOf[ResultSerializationMonotask])
    assert(sink.dependencies.size === 3)
  }
}

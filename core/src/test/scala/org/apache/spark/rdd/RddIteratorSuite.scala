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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark._
import org.apache.spark.monotasks.compute.RddComputeMonotask
import org.apache.spark.storage.{BlockManager, RDDBlockId, StorageLevel}

/**
 * Tests that the RDD class's iterator() method correctly computes the RDD, or retrieves it from
 * the BlockManager is it was cached in memory.
 */
class RddIteratorSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  private var blockManager: BlockManager = _
  private var split: Partition = _
  private var context: TaskContextImpl = _
  /** An RDD which returns the values [1, 2, 3, 4]. */
  private var rddA: RDD[Int] = _
  private var rddB: RDD[Int] = _

  before {
    // Make a new task context for each test so each test gets its own set of task metrics.
    context = new TaskContextImpl(0, 0)

    // Create a SparkContext as a convenient way to construct a BlockManager.
    sc = new SparkContext("local", "test", new SparkConf(false))
    blockManager = sc.env.blockManager
    split = new Partition { override def index: Int = 0 }

    rddA = new RDD[Int](sc, Nil) {
      override def getPartitions: Array[Partition] = Array(split)
      override val getDependencies = List[Dependency[_]]()
      override def compute(split: Partition, context: TaskContext) = Array(1, 2, 3, 4).iterator
    }
    val intermediateRdd = new RDD[Int](sc, List(new OneToOneDependency(rddA))) {
      override def getPartitions: Array[Partition] = firstParent[Int].partitions
      override def compute(split: Partition, context: TaskContext) =
        firstParent[Int].iterator(split, context)
    }.cache()
    rddB = new RDD[Int](sc, List(new OneToOneDependency(intermediateRdd))) {
      override def getPartitions: Array[Partition] = firstParent[Int].partitions
      override def compute(split: Partition, context: TaskContext) =
        firstParent[Int].iterator(split, context)
    }.cache()
  }

  test("iterator: get uncached rdd, store in memory") {
    rddA.persist(StorageLevel.MEMORY_ONLY)
    val computeValue = rddA.iterator(split, context)
    val getValue = blockManager.get(new RDDBlockId(rddA.id, split.index))
    assert(computeValue.toList === List(1, 2, 3, 4))
    assert(getValue.isDefined, "Block cached from getOrCompute is not found!")
    assert(getValue.get.data.toList === List(1, 2, 3, 4))
  }

  /**
   * This test verifies that the RDD class's iterator() method does not cache an RDD on disk even if
   * the RDD's StorageLevel indicates that it should be cached on disk.
   */
  test("iterator: get uncached rdd, store on disk") {
    // Compute the RDD, and indicate that it should be cached on disk.
    rddA.persist(StorageLevel.DISK_ONLY)
    val computeValue = rddA.iterator(split, context)
    assert(computeValue.toList === List(1, 2, 3, 4))

    // Verify that the BlockManager has no knowledge of the RDD, meaning that it was not cached.
    assert(blockManager.getStatus(new RDDBlockId(rddA.id, split.index)).isEmpty)
  }

  test("iterator: get cached rdd") {
    val blockId = new RDDBlockId(rddA.id, split.index)
    val values = Array(5, 6, 7)
    blockManager.cacheIterator(blockId, values.iterator, StorageLevel.MEMORY_ONLY, false)

    rddA.persist(StorageLevel.MEMORY_ONLY)
    val cachedValues = rddA.iterator(split, context)
    assert(cachedValues.toArray === values)
  }

  test("iterator: get uncached local rdd") {
    rddA.persist(StorageLevel.MEMORY_ONLY)
    val values = rddA.iterator(split, new TaskContextImpl(0, 0, runningLocally = true))
    assert(values.toList === List(1, 2, 3, 4))
    // Since the task is running locally, the RDD should not be cached.
    assert(blockManager.getStatus(new RDDBlockId(rddA.id, split.index)).isEmpty)
  }

  test("iterator: verify task metrics updated correctly for in-memory blocks") {
    rddB.iterator(split, context)
    assert(context.taskMetrics.updatedBlocks.getOrElse(Seq()).size === 2)
  }

  test("iterator: doesn't recompute an RDD when it was computed by an earlier RddComputeMonotask") {
    val counter = new AtomicInteger(0)
    val sourceValues = Array(1, 2, 3, 4)
    val testRdd = new RDD[Int](sc, Nil) {
      override def getPartitions: Array[Partition] = Array(split)
      override val getDependencies = List[Dependency[_]]()
      override def compute(split: Partition, context: TaskContext) = {
        counter.incrementAndGet()
        sourceValues.iterator
      }
    }

    // Execute an RddComputeMonotask that will compute testRdd and cache it in memory.
    new RddComputeMonotask(context, testRdd, split).execute()

    // Since testRdd is already cached in memory, these calls to testRdd.iterator() should not
    // recompute it. Call iterator() twice to make sure that the RDD is not recomputed if multiple
    // monotasks need to fetch it.
    assert(testRdd.iterator(split, context).toArray === sourceValues)
    assert(testRdd.iterator(split, context).toArray === sourceValues)
    assert(counter.get() === 1, "RDD should have been computed once.")
  }
}

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

package org.apache.spark

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.monotasks.Monotask
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleHelper}
import org.apache.spark.storage.{BlockManagerId, BlockManagerMaster}

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  val id = Dependency.newId()

  def rdd: RDD[T]

  /**
   * Returns the monotasks that need to be run to construct the data for this dependency.
   *
   * Current, all implementations assume that the monotasks for a dependency do not depend
   * on one another, and that the only dependency is that the compute monotask for the RDD
   * being computed depends on the monotasks for its dependencies.
   */
  def getMonotasks(
    partition: Partition,
    dependencyIdToPartitions: HashMap[Long, HashSet[Partition]],
    context: TaskContextImpl,
    nextMonotask: Monotask)
    : Seq[Monotask]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd

  override def getMonotasks(
    partition: Partition,
    dependencyIdToPartitions: HashMap[Long, HashSet[Partition]],
    context: TaskContextImpl,
    nextMonotask: Monotask)
    : Seq[Monotask] = {
    // For each of the parent partitions, get the input monotasks to generate that partition.
    val partitions = dependencyIdToPartitions.get(this.id)
    if (partitions.isEmpty) {
      throw new SparkException("Missing parent partition information for partition " +
        s"${partition.index} of dependency $this (should have been set in DAGScheduler)")
    } else {
      partitions.get.toArray.flatMap { parentPartition =>
        rdd.buildDag(parentPartition, dependencyIdToPartitions, context, nextMonotask)
      }
    }
  }
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If set to None,
 *                   the default serializer, as specified by `spark.serializer` config option, will
 *                   be used.
 * @param keyOrdering key ordering for RDD's shuffles
 * @param aggregator map/reduce-side aggregator for RDD's shuffle
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
 */
@DeveloperApi
class ShuffleDependency[K, V, C](
    @transient _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Option[Serializer] = None,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  override def rdd = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  val shuffleId: Int = _rdd.context.newShuffleId()

  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.size, this)

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))

  var reduceLocations = new Array[BlockManagerId](partitioner.numPartitions)

  /** Helps with reading the shuffle data associated with this dependency. Set by getMonotasks(). */
  var shuffleHelper: Option[ShuffleHelper[K, V, C]] = None

  override def getMonotasks(
    partition: Partition,
    dependencyIdToPartitions: HashMap[Long, HashSet[Partition]],
    context: TaskContextImpl,
    nextMonotask: Monotask)
    : Seq[Monotask] = {
    // TODO: should the shuffle helper code just be part of the dependency?
    shuffleHelper = Some(new ShuffleHelper(this, partition.index, context))
    val monotasks = shuffleHelper.get.getReadMonotasks()
    monotasks.foreach(nextMonotask.addDependency(_))
    monotasks
  }

  /**
   * Generates, saves, and returns a possible assignment of reduce tasks to machines.
   *
   * This is used to send possible reduce task locations to map tasks, so that they can
   * opportunistically start sending shuffle data early if the network is idle.
   */
  def assignReduceTasksToExecutors(
      blockManagerMaster: BlockManagerMaster): Seq[(BlockManagerId, Seq[Int])] = {
    val numReduceTasks = partitioner.numPartitions
    val possibleReduceTaskLocations =
      blockManagerMaster.getPeers(SparkEnv.get.blockManager.blockManagerId)
    val executorIdToReduceTaskIds = possibleReduceTaskLocations.zipWithIndex.map {
      case (id, index) =>
        val locations = index until numReduceTasks by possibleReduceTaskLocations.size
        (id, locations)
    }

    // Set reduceLocations so that it can be used later by the DAGScheduler.
    executorIdToReduceTaskIds.foreach {
      case (blockManagerId, reduceTaskIds) =>
        reduceTaskIds.foreach { reduceTaskId =>
          reduceLocations(reduceTaskId) = blockManagerId
        }
    }
    executorIdToReduceTaskIds
  }
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int) = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int) = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}

private[spark] object Dependency {
  val nextId = new AtomicLong(0)

  def newId(): Long = nextId.getAndIncrement()

  /**
   * Returns a mapping of NarrowDependencies to the partitions of the parent RDD for that
   * dependency. This should include all NarrowDependencies that will be traversed as part of
   * completing this macrotask and all associated partitions for each NarrowDependency. This is
   * necessary for computing the monotasks for this macrotask: in order to compute the monotasks,
   * we need to walk through all of the rdds and associated partitions that will be computed as part
   * of this task. The NarrowDependency includes a pointer to the associated parent RDD, but the
   * pointer to the parent Partition is stored as part of the child Partition in formats specific
   * to the Partition subclasses, hence the need for this additional mapping.
   *
   * The resulting map is indexed on the Dependency id rather than directly on the dependency
   * because of the way serialization happens. This object is serialized with the Partition,
   * separately from the RDD object, so if Dependency objects were used as keys here, they will end
   * up being different than the Dependency objects in the RDD class, so this mapping would no
   * longer be valid once deserialized.
   */
  // TODO: Write unit test for this!
  def getDependencyIdToPartitions(rdd: RDD[_], partitionIndex: Int)
    : HashMap[Long, HashSet[Partition]] = {
    val dependencyIdToPartitions = new HashMap[Long, HashSet[Partition]]()
    getDependencyIdToPartitionsHelper(rdd, partitionIndex, dependencyIdToPartitions)
    dependencyIdToPartitions
  }

  private def getDependencyIdToPartitionsHelper(
    rdd: RDD[_], partitionIndex: Int, dependencyIdToPartitions: HashMap[Long, HashSet[Partition]]) {
    rdd.dependencies.foreach {
      case narrowDependency: NarrowDependency[_] =>
        val parentPartitions = dependencyIdToPartitions.getOrElseUpdate(
          narrowDependency.id, new HashSet[Partition])

        narrowDependency.getParents(partitionIndex).foreach { parentPartitionIndex =>
          val parentPartition = narrowDependency.rdd.partitions(parentPartitionIndex)
          if (parentPartitions.add(parentPartition)) {
            getDependencyIdToPartitionsHelper(
              narrowDependency.rdd, parentPartitionIndex, dependencyIdToPartitions)
          }
        }

      case _ =>
       // Do nothing: only need partitions for NarrowDependencies.
    }
  }
}

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

import scala.language.existentials

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{AppendOnlyMap, CompactBuffer}

/** The references to rdd and splitIndex are transient because redundant information is stored
  * in the CoGroupedRDD object.  Because CoGroupedRDD is serialized separately from
  * CoGroupPartition, if rdd and splitIndex aren't transient, they'll be included twice in the
  * task closure. */
private[spark] case class NarrowCoGroupSplitDep(
    @transient rdd: RDD[_],
    @transient splitIndex: Int,
    var split: Partition
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

/**
 * Stores information about the narrow dependencies used by a CoGroupedRdd.  narrowDeps maps to
 * the dependencies variable in the parent RDD: for each OneToOneDependency in dependencies,
 * narrowDeps has a NarrowCoGroupSplitDep (describing the partition for that dependency) at the
 * corresponding index.
 */
private[spark] class CoGroupPartition(
    idx: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

/**
 * :: DeveloperApi ::
 * A RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * Note: This is an internal API. We recommend users use RDD.cogroup(...) instead of
 * instantiating this directly.

 * @param rdds parent RDDs.
 * @param part partitioner used to partition the shuffle output
 */
@DeveloperApi
class CoGroupedRDD[K](@transient var rdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner)
  extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {

  // For example, `(k, a) cogroup (k, b)` produces k -> Array(ArrayBuffer as, ArrayBuffer bs).
  // Each ArrayBuffer is represented as a CoGroup, and the resulting Array as a CoGroupCombiner.
  // CoGroupValue is the intermediate state of each value before being merged in compute.
  private type CoGroup = CompactBuffer[Any]
  private type CoGroupValue = (Any, Int)  // Int is dependency number
  private type CoGroupCombiner = Array[CoGroup]

  private var serializer: Option[Serializer] = None

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    this.serializer = Option(serializer)
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_ <: Product2[K, _]] =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, CoGroupCombiner](rdd, part, serializer)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.size

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, _]], Int)]
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      case oneToOneDependency: OneToOneDependency[_] =>
        val dependencyPartition = split.narrowDeps(depNum).get.split
        // Read them from the parent
        val it = oneToOneDependency.asInstanceOf[OneToOneDependency[Product2[K, _]]].rdd.iterator(
          dependencyPartition, context)
        rddIterators += ((it, depNum))

      case shuffleDependency: ShuffleDependency[_, _, _] =>
        // Read map outputs of the shuffle
        val it = shuffleDependency.asInstanceOf[ShuffleDependency[K, _, _]].shuffleHelper match {
          case Some(shuffleHelper) =>
            shuffleHelper.getDeserializedAggregatedSortedData()
          case None =>
            throw new SparkException(
              s"No shuffle reader found for shuffle ${shuffleDependency.shuffleId} (should have " +
                "been set when creating the monotasks for this Macrotask)")
        }
        rddIterators += ((it, depNum))
    }

    val map = new AppendOnlyMap[K, CoGroupCombiner]
    val update: (Boolean, CoGroupCombiner) => CoGroupCombiner = (hadVal, oldVal) => {
      if (hadVal) oldVal else Array.fill(numRdds)(new CoGroup)
    }
    val getCombiner: K => CoGroupCombiner = key => {
      map.changeValue(key, update)
    }
    rddIterators.foreach { case (it, depNum) =>
      while (it.hasNext) {
        val kv = it.next()
        getCombiner(kv._1)(depNum) += kv._2
      }
    }
    new InterruptibleIterator(context,
      map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}

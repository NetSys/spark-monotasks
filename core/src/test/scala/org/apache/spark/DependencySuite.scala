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

import scala.collection.mutable.HashSet

import org.scalatest.FunSuite

// This import is necessary for reduceByKey to work (via implicit conversions).
import org.apache.spark.SparkContext._

class DependencySuite extends FunSuite with SharedSparkContext {
  test("getDependencyIdToPartitions: empty map returned for RDDs with no dependencies") {
    val numPartitions = 5
    val rdd = sc.makeRDD(List[(Int, Int)]((1, 2), (1, 4), (3, 1), (4, 5)))
    val dependencyIdToPartitions = Dependency.getDependencyIdToPartitions(rdd, 2)
    assert(dependencyIdToPartitions.isEmpty,
      "RDD has no dependencies, so no partitions need to be tracked")
  }

  test("getDependencyIdToPartitions: empty map returned for ShuffleDependency") {
    val numPartitions = 2
    // Create a ShuffledRDD and ensure that getDependencyIdToPartitions returns an empty map.
    val rdd = sc.makeRDD(List[(Int, Int)]((1, 2), (1, 4), (3, 1), (4, 5)))
    val shuffledRdd = rdd.reduceByKey(_ + _)

    val dependencyIdToPartitions = Dependency.getDependencyIdToPartitions(shuffledRdd, 1)
    assert(dependencyIdToPartitions.isEmpty,
      "ShuffledRDD has no narrow dependencies, so no partitions need to be tracked")
  }

  test("getDependencyIdToPartitions: single mapping returned for single OneToOneDependencies") {
    val numPartitions = 3
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), numPartitions)
    val mappedRdd = rdd.map(_ + 5)

    (0 until numPartitions).foreach { partitionId =>
      val dependencyIdToPartitions = Dependency.getDependencyIdToPartitions(mappedRdd, partitionId)
      assert(dependencyIdToPartitions.size === 1)

      assert(dependencyIdToPartitions.contains(mappedRdd.dependencies(0).id))
      val partitionSet = new HashSet[Partition]()
      partitionSet.add(rdd.partitions(partitionId))
      assert(dependencyIdToPartitions(mappedRdd.dependencies(0).id) === partitionSet)
    }
  }

  test("getDependencyIdToPartitions: correctly handles chain of dependencies") {
    val numPartitions = 3
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), numPartitions)
    val mappedRdd = rdd.map(_ + 5)
    val filteredRdd = mappedRdd.filter(_ % 2 == 1)

    (0 until numPartitions).foreach { partitionId =>
      val dependencyIdToPartitions = Dependency.getDependencyIdToPartitions(filteredRdd, partitionId)
      assert(dependencyIdToPartitions.size === 2)

      val baseRddSet = new HashSet[Partition]()
      baseRddSet.add(rdd.partitions(partitionId))
      assert(dependencyIdToPartitions.valuesIterator.contains(baseRddSet),
        "One partition should be from the base RDD")

      val mappedRddSet = new HashSet[Partition]()
      mappedRddSet.add(mappedRdd.partitions(partitionId))
      assert(dependencyIdToPartitions.valuesIterator.contains(mappedRddSet),
        "One partition should be from the mapped RDD")
    }
  }

  test("getDependencyIdToPartitions: correctly handles RDDs with multiple narrow dependencies") {
    val numPartitions1 = 3
    val numPartitions2 = 5
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5), numPartitions1)
    val rdd2 = sc.makeRDD(List(6, 7, 8), numPartitions2)
    val cartesianRdd = rdd1.cartesian(rdd2)

    (0 until (numPartitions1 * numPartitions2)).foreach { partitionIndex =>
      val dependencyIdToPartitions = Dependency.getDependencyIdToPartitions(
        cartesianRdd, partitionIndex)
      val errorMessage =
        "CartesianRDD should result in two saved partitions: one for each dependency"
      assert(dependencyIdToPartitions.size === 2, errorMessage)

      // Make sure there's one partition for each of the parents.
      val partitionsFromRdd1 = dependencyIdToPartitions.values.filter { partitionSet =>
        partitionSet.size == 1 && partitionSet.filter(rdd1.partitions.contains(_)).size == 1
      }
      assert(partitionsFromRdd1.size === 1)
      val partitionsFromRdd2 = dependencyIdToPartitions.values.filter { partitionSet =>
        partitionSet.size == 1 && partitionSet.filter(rdd2.partitions.contains(_)).size == 1
      }
      assert(partitionsFromRdd2.size === 1)
    }
  }
}

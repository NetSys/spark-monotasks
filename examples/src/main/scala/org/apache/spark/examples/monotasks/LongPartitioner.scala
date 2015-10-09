/*
 * Copyright 2015 The Regents of The University California
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

package org.apache.spark.examples.monotasks

import org.apache.spark.Partitioner

/**
 * Partitioner that evenly divides the space of all Longs. Useful to avoid sampling data (which
 * generates an extra stage) when sorting a dataset that uses Longs for the keys.
 */
class LongPartitioner(private val partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  private val partitionSize = (Long.MaxValue.toFloat - Long.MinValue.toFloat) / partitions

  override def getPartition(key: Any): Int = {
    val partition = (key.asInstanceOf[Long].toDouble - Long.MinValue.toDouble) / partitionSize
    return Math.min(partition.floor.toInt, numPartitions - 1)
  }
}

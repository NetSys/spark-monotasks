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

import java.nio.ByteBuffer

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.storage.{BlockManagerId, StorageLevel, ShuffleBlockId}

/**
 * A ShuffleMapTask (SMT) divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency). Note that the unitask changes to SMT have made it so that
 * SMT _must_ operate on an RDD of type RDD[(Int, ByteBuffer)] -- pairs of (reducePartitionId,
 * serializedBytes) and that it requires that all bytes for one partition already be grouped together
 * (i.e., reducePartitionId must be unique). SMT will then write out the serialized bytes to the shuffle
 * block store directly.
 *
  * See [[org.apache.spark.scheduler.Task]] for more information.
  *
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation])
  extends Task[MapStatus](stageId, partition.index) with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, null, new Partition { override def index = 0 }, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[(Int, ByteBuffer)], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    try {
      val store = SparkEnv.get.blockManager.diskStore
      val compressedSizes = new Array[Byte](dep.partitioner.numPartitions)

      for ((outPartitionId, bytes) <- rdd.iterator(partition, context)) {
        val outBlockId = ShuffleBlockId(dep.shuffleId, partitionId, outPartitionId)
        store.putBytes(outBlockId, bytes, StorageLevel.DISK_ONLY)
        assert(compressedSizes(outPartitionId) == 0) // only write 1 output per output partition
        compressedSizes(outPartitionId) = MapOutputTracker.compressSize(bytes.limit()) //TODO(ryan): not sure on limit() validity
      }
      new MapStatus(SparkEnv.get.blockManager.blockManagerId, compressedSizes)
    } finally {
      rdd.free(partition)
      context.executeOnCompleteCallbacks()
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}

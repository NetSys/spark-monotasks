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
import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.storage.{BlockManagerId, StorageLevel}
import java.io.Externalizable

/**
* A ReadTask blindly caches the elements of a partition in memory
*
 * The idea is to devote a task to simply bringing in a block into memory.
 * So, we'll want to call this only on an RDD that reads blocks from disk,
 * and will want to ideally call it on the same node that the data resides
 * on.
 *
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ReadTask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation])
  extends Task[ReadStatus](stageId, partition.index) with Logging {

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): ReadStatus = {
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val rdd = ser.deserialize[BlockRDD[_]] (
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
      // TODO(ryan) where is the ser/deser defined for this task?

    metrics = Some(context.taskMetrics)
    try {
      val manager = SparkEnv.get.cacheManager
      manager.getOrCompute(rdd, partition, context, StorageLevel.MEMORY_ONLY_SER) // cache the partition (hopefully :)
      new ReadStatus(SparkEnv.get.blockManager.blockManagerId, 0)
      // TODO(ryan): is blockManagerId enough to ident the machine?
      // TODO(ryan): need a way to get byte count for arg 2
    } finally {
      context.executeOnCompleteCallbacks() // TODO(ryan) what does this do?
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ReadTask(%d, %d)".format(stageId, partitionId)
}

class ReadStatus(val location: BlockManagerId, val bytesTransferred: Long) extends Serializable

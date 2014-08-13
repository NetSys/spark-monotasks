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
import org.apache.spark.storage.{ShuffleBlockId, BlockManagerId, StorageLevel}
import java.io.Externalizable
import com.sun.istack.internal.NotNull

/**
 * A PipelineTask blindly caches the elements of a partition in memory
 *
 * The idea is to devote a task to simply bringing in a block into memory.
 * So, we'll want to call this only on an RDD that reads blocks from disk,
 * and will want to ideally call it on the same node that the data resides
 * on.
 *
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class MiniFetchTask(stageId: Int,
                                   taskBinary: Broadcast[Array[Byte]])
  extends Task[Unit](stageId, -1) with Logging {
  // TODO(ryan) partition doesn't make sense, using -1 for now ...

  override def runTask(context: TaskContext) {
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val blockId = ser.deserialize[ShuffleBlockId] (
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    try {
      val bytes = SparkEnv.get.blockManager.getRemoteBytes(blockId).get
      // TODO(ryan): look into if things are getting unnecessarily serialized/deserialized
      SparkEnv.get.blockManager.memoryStore.putBytes(blockId, bytes, StorageLevel.MEMORY_ONLY_SER)
    } finally {
      context.executeOnCompleteCallbacks() // TODO(ryan) what does this do?
    }
  }

  override def toString = "MiniFetchTask(%d)".format(stageId)

}

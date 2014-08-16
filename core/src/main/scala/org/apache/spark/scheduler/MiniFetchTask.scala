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
import org.apache.spark.serializer.Serializer

/**
 * A MiniFetchTasks fetches _one_ partition of a ShuffleMapTask output for one
 * partition of a downstream RDD. This is in contrast to an ordinary shuffle
 * that fetches _all_ parts of ShuffleMapTasks for one downstream RDD partition.
 *
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD
 */
private[spark] class MiniFetchTask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    val shuffleBlockId: ShuffleBlockId)
  extends Task[Unit](stageId, -1) with Logging {
  // TODO(ryan) partition doesn't make sense, using -1 for now ...

  override def runTask(context: TaskContext) {
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (blockId, manager, compSize, dep) = ser.deserialize[(ShuffleBlockId, BlockManagerId, Byte, MiniFetchDependency[_, _, _])] (
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    try {
      val length: Long = MapOutputTracker.decompressSize(compSize)
      val iterator = SparkEnv.get.blockManager.getMultiple(Seq((manager, Seq((blockId, length)))),
        Serializer.getSerializer(dep.serializer)).next()._2.get
      // TODO(ryan): pretty sure things are getting unnecessarily serialized/deserialized
      SparkEnv.get.blockManager.memoryStore.putIterator(blockId, iterator, StorageLevel.MEMORY_ONLY_SER, false)
    } finally {
      context.executeOnCompleteCallbacks()
    }
  }

  override def toString = "MiniFetchTask(%d)".format(stageId)

}

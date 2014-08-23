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

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId, WarmedShuffleBlockId}

/**
 * A MiniFetchWarmTask warms (brings into memory) part of a partition
 * written by a previous ShuffleMapTask
 *
 * @param stageId id of the stage this task belongs to
 */
private[spark] class MiniFetchWarmTask(
    stageId: Int,
    val shuffleBlockId: ShuffleBlockId,
    val blockManagerId: BlockManagerId)
  extends Task[Unit](stageId, -1) with Logging {
  // TODO(ryan) partition doesn't make sense, using -1 for now ...

  override def runTask(context: TaskContext) {
    // Deserialize the RDD using the broadcast variable.
    metrics = Some(context.taskMetrics)
    try {
      val bytes = SparkEnv.get.blockManager.diskStore.getBytesDirect(shuffleBlockId)
      val warmedId = WarmedShuffleBlockId.fromShuffle(shuffleBlockId)
      SparkEnv.get.blockManager.memoryStore.putBytesDirect(warmedId, bytes)
    } finally {
      context.executeOnCompleteCallbacks()
    }
  }

  override def toString = "MiniFetchWarmTask(%d)".format(stageId)

}

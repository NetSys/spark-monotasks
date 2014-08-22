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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, PipelineDependency, Partition, TaskContext}
import org.apache.spark.storage.{PipelinedBlockId, RDDBlockId, StorageLevel}

/**
 * A PipelinedRDD represents a soft-barrier between the parent RDD and the
 * calling RDD. The scheduler will create a PipelineTask that caches the
 * parent's RDD partitions to memory. Then, when compute() is called here, the
 * partition will already be in memory.
 *
 * The pipelining is intended to happen within a stage, between tasks of different
 * MiniStages
 */
private[spark] class PipelinedRDD[T: ClassTag](
    prev: RDD[T])
  extends RDD[T](prev.context , List(new PipelineDependency(prev))) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = prev.partitioner    // Since pipeline is a logical identity function

  private def blockId(partition: Partition) = PipelinedBlockId(prev.id, partition.index)

  override def compute(split: Partition, context: TaskContext) = {
    SparkEnv.get.blockManager.memoryStore.getValues(blockId(split)).get.asInstanceOf[Iterator[T]]
  }

  override def resource = RDDResourceTypes.None

  override def free(partition: Partition) {
    SparkEnv.get.blockManager.memoryStore.remove(blockId(partition))
    // Note that we _don't_ continue the recursive frees to our dependency because it should have been freed
    // already, if it needed to be
  }
}

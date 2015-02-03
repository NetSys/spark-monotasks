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

package org.apache.spark.monotasks.compute

import java.lang.IllegalStateException
import java.nio.ByteBuffer

import org.apache.spark.{SparkEnv, TaskContextImpl}
import org.apache.spark.storage.{BlockId, MonotaskResultBlockId, StorageLevel}

/**
 * Reads the specified block from the BlockManager, serializes it, and stores the result back in the
 * BlockManager using a MonotaskResultBlockId.
 */
private[spark] class SerializationMonotask(context: TaskContextImpl, blockId: BlockId)
  extends ComputeMonotask(context) {

  resultBlockId = Some(new MonotaskResultBlockId(taskId))

  override def execute(): Option[ByteBuffer] = {
    val blockManager = SparkEnv.get.blockManager
    blockManager.get(blockId).map { blockResult =>
      val buffer = blockManager.dataSerialize(blockId, blockResult.data, SparkEnv.get.serializer)
      blockManager.cacheBytes(getResultBlockId(), buffer, StorageLevel.MEMORY_ONLY_SER, false)
    }.getOrElse {
      throw new IllegalStateException(s"Could not serialize block $blockId because it could not " +
        "be found in memory.")
    }
    None
  }
}

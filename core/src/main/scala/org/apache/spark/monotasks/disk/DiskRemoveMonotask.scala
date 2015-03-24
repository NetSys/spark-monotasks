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

package org.apache.spark.monotasks.disk

import org.apache.spark.TaskContextImpl
import org.apache.spark.storage.BlockId

/** Contains the parameters and logic necessary to remove a single block from a single disk. */
private[spark] class DiskRemoveMonotask(
    context: TaskContextImpl, blockId: BlockId, val diskId: String)
  extends DiskMonotask(context, blockId) {

  override def execute(): Unit =
    blockManager.blockFileManager.getBlockFile(blockId, diskId).map(_.delete())
}

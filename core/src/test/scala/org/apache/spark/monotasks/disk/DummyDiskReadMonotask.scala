/*
 * Copyright 2016 The Regents of The University California
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

/**
 * A simplified DiskMonotask class that keeps track of which disks are currently being accessed and
 * the time at which each instance's execution started. DummyDiskReadMonotask is a subclass of
 * DiskReadMonotask and requires a diskId to be known in advance. This class does no meaningful
 * work, so it should only be used for testing purposes.
 */
private[spark] class DummyDiskReadMonotask(
    taskContext: TaskContextImpl,
    blockId: BlockId,
    diskId: String,
    val taskTime: Long)
  extends DiskReadMonotask(taskContext, blockId, diskId){

  override def execute(): Unit = {
    DiskMonotaskTestHelper.executeTaskOnDisk(taskId, diskId, taskTime)
  }
}
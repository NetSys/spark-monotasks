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

import org.apache.spark.{SparkEnv, TaskContextImpl}
import org.apache.spark.monotasks.Monotask
import org.apache.spark.storage.BlockId

/**
 * A DiskMonotask encapsulates the parameters and logic of a single disk operation. Each type of
 * disk operation is represented by a different subclass of DiskMonotask. Subclasses contain logic
 * for interacting with physical disks.
 */
private[spark] abstract class DiskMonotask(context: TaskContextImpl, val blockId: BlockId)
  extends Monotask(context) {

  val blockManager = SparkEnv.get.blockManager

  /**
   * Executes this DiskMonotask by interacting with a single physical disk. Throws an exception if
   * the disk operation cannot be completed.
   */
  def execute(): Unit
}

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

/** This is the superclass of all DiskMonotasks that operate on HDFS files. */
private[spark] abstract class HdfsDiskMonotask(taskContext: TaskContextImpl, blockId: BlockId)
  extends DiskMonotask(taskContext, blockId) {

  /**
   * Returns the Spark local directory that is located on the same physical disk as the HDFS path
   * that this monotask will access.
   *
   * @param sparkLocalDirs The Spark local directories. One of these will be returned. This is
   *                       assumed to be non-empty.
   */
  def chooseLocalDir(sparkLocalDirs: Seq[String]): String
}

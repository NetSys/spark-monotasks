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

package org.apache.spark.monotasks

import org.apache.spark.SparkException
import org.apache.spark.monotasks.compute.ComputeMonotask
import org.apache.spark.monotasks.disk.DiskMonotask
import org.apache.spark.monotasks.network.NetworkMonotask

/** Types of monotasks (used to manage the queues for each resource). */
private[spark] object MonotaskType extends Enumeration {
  type MonotaskType = Value
  val Compute, Disk, Network = Value

  def getType(monotask: Monotask): MonotaskType.Value = {
    monotask match {
      case _: ComputeMonotask =>
        Compute
      case _: DiskMonotask =>
        Disk
      case _: NetworkMonotask =>
        Network
      case otherMonotask: Any =>
        throw new SparkException(s"No type available for monotask $otherMonotask")
    }
  }
}

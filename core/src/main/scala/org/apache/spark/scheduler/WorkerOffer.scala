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

/**
 * Represents free resources available on an executor.
 */
private[spark]
case class WorkerOffer(executorId: String, host: String, resources: Resources) {

  def this(executorId: String, host: String, cores: Int) {
    // TODO(ryan): will want to warn/depricate this to make sure we don't miss its usages
    this(executorId, host, Resources.fromCores(cores))
  }

  def without(resources: Resources) = WorkerOffer(executorId, host, this.resources - resources)

}

/** A wrapper for the actual resource types */
case class Resources(cores: Int, networkSlots: Int, disks: Set[Int]) {

   assert(isSane(), "cores is %d, slots is %d" format (cores, networkSlots))

  def +(other: Resources) = {
    Resources(cores + other.cores, networkSlots + other.networkSlots, disks ++ other.disks)
  }

  def -(other: Resources) = {
    assert(canFulfill(other), "%s can't fulfill %s" format (this, other))
    Resources(cores - other.cores, networkSlots - other.networkSlots, disks -- other.disks)
  }

  /** Can this resource offer fulfill all the requirements of another? */
  def canFulfill(other: Resources): Boolean = {
    cores >= other.cores && networkSlots >= other.networkSlots && other.disks.subsetOf(disks)
  }

  private def isSane() = (cores >= 0 && networkSlots >= 0)

}

object Resources {

  /** To ease backward compatibility, pretend that only cores are required*/
  def fromCores(cores: Int) = Resources(cores, 0, Set())

}
// TODO(ryan): network slots are all the same, so its just a count, but disks need an id (using an int for now...)

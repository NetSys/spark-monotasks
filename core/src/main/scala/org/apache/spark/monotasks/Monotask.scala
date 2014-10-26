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

package org.apache.spark.monotasks

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{ArrayBuffer, HashSet}

/**
 * A Monotask object encapsulates information about an operation that uses only one type of
 * resource. Subclasses contain task information specific to the resource that will be operated on,
 * and may include methods to actually interact with a resource.
 */
private[spark] abstract class Monotask(val localDagScheduler: LocalDagScheduler) {
  val taskId = Monotask.newId()

  // IDs of Monotasks that must complete before this can be run.
  val dependencies = new HashSet[Long]()

  // Monotasks that require this monotask to complete before they can be run.
  val dependents = new ArrayBuffer[Monotask]()

  /**
   * Registers a monotask that must complete before this monotask can be run (by updating state
   * in both this monotask and in the passed dependency).
   */
  def addDependency(dependency: Monotask) {
    dependencies += dependency.taskId
    dependency.dependents += this
  }
}

private[spark] object Monotask {
  val nextId = new AtomicLong(0)

  def newId(): Long = nextId.getAndIncrement()
}

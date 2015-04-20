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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContextImpl
import org.apache.spark.storage.{BlockId, MonotaskResultBlockId}

/**
 * A Monotask object encapsulates information about an operation that uses only one type of
 * resource. Subclasses contain task information specific to the resource that will be operated on,
 * and may include methods to actually interact with a resource.
 *
 * Monotasks are responsible for notifying the localDagScheduler when they have completed
 * successfully or when they have failed.
 */
private[spark] abstract class Monotask(val context: TaskContextImpl) {
  val taskId = Monotask.newId()

  // Whether this monotask has finished executing.
  private var isFinished = false

  // The BlockId with which this monotask stored temporary data in the BlockManager for use by its
  // dependencies, or None if the monotask did not store any temporary data.
  protected var resultBlockId: Option[BlockId] = None

  // Monotasks that need to have finished before this monotask can be run. This includes all
  // dependencies (even ones that have already completed) so that we can clean up intermediate data
  // produced by dependencies when this monotask completes. Monotasks that are added to this data
  // structure should never be removed.
  val dependencies = new ArrayBuffer[Monotask]()

  // Monotasks that require this monotask to complete before they can be run. Monotasks that are
  // added to this data structure should never be removed.
  val dependents = new ArrayBuffer[Monotask]()

  /**
   * Registers a monotask that must complete before this monotask can be run (by updating state
   * in both this monotask and in the passed dependency).
   */
  def addDependency(dependency: Monotask) {
    dependencies += dependency
    dependency.dependents += this
  }

  def getResultBlockId(): BlockId = {
    resultBlockId.getOrElse(throw new UnsupportedOperationException(
      s"Monotask $this (id: ${taskId}) does not have a result BlockId."))
  }

  /**
   * Returns true if all of the monotasks that this monotask depends on have completed (meaning
   * that this monotask can now be scheduled), or false otherwise.
   */
  def dependenciesSatisfied() =
    dependencies.filter(!_.isFinished).isEmpty

  /**
   * Marks this monotask as completed, and, if possible, cleans up its dependencies. A dependency
   * monotask can be cleaned up (its temporary data can be deleted and it can be removed from the
   * DAG of monotasks) if and only if all of its dependents have finished. This should be called
   * after this monotask either completes or fails.
   */
  def cleanup() {
    isFinished = true
    dependencies.filter(_.dependents.filter(!_.isFinished).isEmpty).foreach { dependency =>
      dependency.resultBlockId.map { blockId =>
        val tellMaster = blockId match {
          case monotaskResultBlockId: MonotaskResultBlockId => false
          case _ => true
        }

        context.env.blockManager.removeBlockFromMemory(blockId, tellMaster)
      }
    }
  }
}

private[spark] object Monotask {
  val nextId = new AtomicLong(0)

  def newId(): Long = nextId.getAndIncrement()
}

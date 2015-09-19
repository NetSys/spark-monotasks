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

import scala.util.control.NonFatal

import org.mockito.Mockito.mock

import org.apache.spark.SparkEnv
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.storage.{BlockFileManager, MemoryStore}

/**
 * A wrapped version of LocalDagScheduler for use in testing.  This class wraps LocalDagScheduler
 * with a method to block waiting for an event to complete.
 */
class LocalDagSchedulerWithSynchrony(
    executorBackend: ExecutorBackend, blockFileManager: BlockFileManager)
  extends LocalDagScheduler(blockFileManager, SparkEnv.get.conf) {

  initialize(executorBackend, mock(classOf[MemoryStore]))

  def runEvent(event: LocalDagSchedulerEvent) {
    try {
      // Forward event to `onReceive` directly to avoid processing event asynchronously.
      onReceive(event)
    } catch {
      case NonFatal(e) => onError(e)
    }
  }
}
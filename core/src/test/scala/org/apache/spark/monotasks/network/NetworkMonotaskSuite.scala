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

package org.apache.spark.monotasks.network

import scala.concurrent.{Await, future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, verify, when}

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.network.{BufferMessage, ConnectionManager}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, MemoryStore, ShuffleBlockId}

class NetworkMonotaskSuite extends FunSuite with Matchers {

  /**
   * This test had a ton of mocks to ensure something very simple: that when data is successfully
   * read over the network, the result is stored in the memory store and the LocalDagScheduler
   * is notified that the task completed successfully.
   */
  test("execute(): on success, result stored in memory store") {
    // Create the TaskContext used the the NetworkMonotask.
    val env = mock(classOf[SparkEnv])
    val localDagScheduler = mock(classOf[LocalDagScheduler])
    val context = new TaskContext(env, localDagScheduler, 0, null, 0, false)

    // Create the NetworkMonotask.
    val testBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    val blockIds = Array[(BlockId, Long)](
      (ShuffleBlockId(0,0,0), 5),
      (ShuffleBlockId(0,1,0), 12),
      (ShuffleBlockId(0,2,0), 15))
    val networkMonotask = new NetworkMonotask(context, testBlockManagerId, blockIds)

    // Create the future to give to the monotask (via a mock) for the network call.
    val bufferMessage = mock(classOf[BufferMessage])
    val f = future(bufferMessage)

    // Set up all the mocks so that the call to send a message (from the NetworkMonotask) returns
    // the future defined above.
    val blockManager = mock(classOf[BlockManager])
    when(env.blockManager).thenReturn(blockManager)
    val connectionManager = mock(classOf[ConnectionManager])
    when(blockManager.connectionManager).thenReturn(connectionManager)
    when(connectionManager.sendMessageReliably(any(), any())).thenReturn(f)
    when(blockManager.futureExecContext).thenReturn(global)
    val memoryStore = mock(classOf[MemoryStore])
    when(blockManager.memoryStore).thenReturn(memoryStore)

    // Execute the network monotask and wait for the future to complete.
    networkMonotask.execute()
    Await.result(f, 5000 millis);

    // This is the important part of the test: making sure the resulting data was put in the memory
    // store and that the LocalDagScheduler was notified of the task's completion.
    verify(memoryStore).putValue(networkMonotask.resultBlockId, bufferMessage)
    verify(localDagScheduler).handleTaskCompletion(networkMonotask)
  }
}

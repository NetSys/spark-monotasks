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

import org.mockito.Mockito._

import org.scalatest.FunSuite

import org.apache.spark.{SparkEnv, TaskContextImpl}

class DeficitRoundRobinQueueSuite extends FunSuite {
  // Set SparkEnv, because the DiskMonotask constructor uses it.
  val sparkEnv = mock(classOf[SparkEnv])
  SparkEnv.set(sparkEnv)

  def getMonotask(virtualSize: Double): DiskMonotask = {
    // The monotask won't actually be executed, so none of the values matter.
    val monotask = new DiskReadMonotask(new TaskContextImpl(0, 0), null, "")
    monotask.virtualSize = virtualSize
    monotask
  }

  test("equivalent to round robin when queues full and items same size") {
    val queue = new DeficitRoundRobinQueue[String]()
    val monotaska1 = getMonotask(1)
    val monotaska2 = getMonotask(1)
    val monotaskb1 = getMonotask(1)
    val monotaskb2 = getMonotask(1)
    val monotaskc1 = getMonotask(1)
    val monotaskc2 = getMonotask(1)

    queue.enqueue("a", monotaska1)
    queue.enqueue("b", monotaskb1)
    queue.enqueue("c", monotaskc1)
    queue.enqueue("c", monotaskc2)
    queue.enqueue("b", monotaskb2)
    queue.enqueue("a", monotaska2)

    // Monotasks should be dequeued in round-robin order.  The order here is based on the fact that
    // keys are iterated over in the same order they're added in.
    assert(queue.dequeue() === monotaska1)
    assert(queue.dequeue() === monotaskb1)
    assert(queue.dequeue() === monotaskc1)
    assert(queue.dequeue() === monotaska2)

    // Add some more monotasks midway through removing them. These should still be removed in
    // round-robin order.
    val monotaska3 = getMonotask(1)
    val monotaskb3 = getMonotask(1)
    val monotaskc3 = getMonotask(1)
    queue.enqueue("b", monotaskb3)
    queue.enqueue("a", monotaska3)
    queue.enqueue("c", monotaskc3)

    assert(queue.dequeue() === monotaskb2)
    assert(queue.dequeue() === monotaskc2)
    assert(queue.dequeue() === monotaska3)
    assert(queue.dequeue() === monotaskb3)
    assert(queue.dequeue() === monotaskc3)
  }

  test("items of different size handled correctly") {
    val queue = new DeficitRoundRobinQueue[String]()
    val monotaska1 = getMonotask(0.25)
    queue.enqueue("a", monotaska1)
    val monotaska2 = getMonotask(0.25)
    queue.enqueue("a", monotaska2)

    val monotaskb1 = getMonotask(1)
    queue.enqueue("b", monotaskb1)

    val monotaskc1 = getMonotask(0.5)
    queue.enqueue("c", monotaskc1)
    val monotaskc2 = getMonotask(0.5)
    queue.enqueue("c", monotaskc2)

    // Here, the quantum should be set to 0.25, so only the first a one should be dequeued.
    assert(queue.dequeue() === monotaska1)
    // The quantum should again be set to 0.25. This should lead to another one of a and then the
    // first one of c getting deqeueued.
    assert(queue.dequeue() === monotaska2)
    assert(queue.dequeue() === monotaskc1)

    // Now things get tricky. Make a new queue with an item of size 0.75. This shouldn't have any
    // accumulated quantum.  The next quantum size should be 0.5. leading to b getting dequeued,
    // along with something else from c.
    val monotaskd1 = getMonotask(0.75)
    queue.enqueue("d", monotaskd1)
    assert(queue.dequeue() === monotaskb1)
    assert(queue.dequeue() === monotaskc2)

    // Now, d1 is the only thing left, so it should be dequeued.
    assert(queue.dequeue() === monotaskd1)
  }

  test("quantum size adjusts correctly") {
    val queue = new DeficitRoundRobinQueue[String]()
    // Start with only big things in the queue for a and b. They should be run in round-robin
    // fashion.
    val monotaska1 = getMonotask(1)
    queue.enqueue("a", monotaska1)
    val monotaska2 = getMonotask(1)
    queue.enqueue("a", monotaska2)

    val monotaskb1 = getMonotask(1)
    queue.enqueue("b", monotaskb1)
    val monotaskb2 = getMonotask(1)
    queue.enqueue("b", monotaskb2)

    assert(queue.dequeue() === monotaska1)
    assert(queue.dequeue() === monotaskb1)

    // Now add a bunch of small things. The quantum size should adjust so they're still run in
    // round-robin fashion (as opposed to running a big batch for each one).
    val monotaska3 = getMonotask(0.1)
    queue.enqueue("a", monotaska3)
    val monotaska4 = getMonotask(0.1)
    queue.enqueue("a", monotaska4)

    val monotaskb3 = getMonotask(0.1)
    queue.enqueue("b", monotaskb3)
    val monotaskb4 = getMonotask(0.1)
    queue.enqueue("b", monotaskb4)

    assert(queue.dequeue() === monotaska2)
    assert(queue.dequeue() === monotaskb2)
    assert(queue.dequeue() === monotaska3)
    assert(queue.dequeue() === monotaskb3)
    assert(queue.dequeue() === monotaska4)
    assert(queue.dequeue() === monotaskb4)
  }

  test("multiple items dequeued in one round when appropriate") {
    val queue = new DeficitRoundRobinQueue[String]()
    // Tests a case where multiple items need to be removed from a queue in one "round" through
    // all of the keys.
    // Start with only "a" having something in the queue, so the quantum will be set to the
    // size of the head of a's queue.
    val monotaska1 = getMonotask(1)
    queue.enqueue("a", monotaska1)
    val monotaska2 = getMonotask(1)
    queue.enqueue("a", monotaska2)
    val monotaska3 = getMonotask(1)
    queue.enqueue("a", monotaska3)

    assert(queue.dequeue() === monotaska1)

    // Then, after dequeueing something for a, add some smaller items for "b".  These should all
    // be dequeued.
    val monotaskb1 = getMonotask(0.25)
    queue.enqueue("b", monotaskb1)
    val monotaskb2 = getMonotask(0.25)
    queue.enqueue("b", monotaskb2)
    val monotaskb3 = getMonotask(0.25)
    queue.enqueue("b", monotaskb3)
    val monotaskb4 = getMonotask(0.25)
    queue.enqueue("b", monotaskb4)
    val monotaskb5 = getMonotask(0.25)
    queue.enqueue("b", monotaskb5)
    val monotaskb6 = getMonotask(0.25)
    queue.enqueue("b", monotaskb6)
    val monotaskb7 = getMonotask(0.25)
    queue.enqueue("b", monotaskb7)
    val monotaskb8 = getMonotask(0.25)
    queue.enqueue("b", monotaskb8)
    val monotaskb9 = getMonotask(0.25)
    queue.enqueue("b", monotaskb9)

    // Add some things for c too.
    val monotaskc1 = getMonotask(0.5)
    queue.enqueue("c", monotaskc1)
    val monotaskc2 = getMonotask(0.5)
    queue.enqueue("c", monotaskc2)
    val monotaskc3 = getMonotask(0.5)
    queue.enqueue("c", monotaskc3)
    val monotaskc4 = getMonotask(0.5)
    queue.enqueue("c", monotaskc4)

    // 4 tasks for b should be dequeued before any tasks for a or c.
    assert(queue.dequeue() === monotaskb1)
    assert(queue.dequeue() === monotaskb2)
    assert(queue.dequeue() === monotaskb3)
    assert(queue.dequeue() === monotaskb4)

    // 2 tasks for c should be dequeued.
    assert(queue.dequeue() === monotaskc1)
    assert(queue.dequeue() === monotaskc2)

    // Now the next task dequeued should be for b -- since the new quanta should be 0.25, so only
    // b can launch.
    assert(queue.dequeue() === monotaskb5)
    // In the next round, the quanta should again be 0.25, allowing b to launch 1, and c to
    // have enough accumulated credit to launch.
    assert(queue.dequeue() === monotaskb6)
    assert(queue.dequeue() === monotaskc3)
    // Now the quanta is 0.5 again, so there's only enough for b.
    assert(queue.dequeue() === monotaskb7)
    // Now a should have accumulated enough deficit to run one.
    assert(queue.dequeue() === monotaska2)
    assert(queue.dequeue() === monotaskb8)
    assert(queue.dequeue() === monotaskc4)
  }

  test("deficit set to 0 when queue is empty") {
    // Start with both a and b with things in the queues.
    val queue = new DeficitRoundRobinQueue[String]()
    val monotaska1 = getMonotask(1)
    queue.enqueue("a", monotaska1)
    val monotaska2 = getMonotask(1)
    queue.enqueue("a", monotaska2)

    val monotaskb1 = getMonotask(1)
    queue.enqueue("b", monotaskb1)
    val monotaskb2 = getMonotask(1)
    queue.enqueue("b", monotaskb2)
    val monotaskb3 = getMonotask(1)
    queue.enqueue("b", monotaskb3)
    val monotaskb4 = getMonotask(1)
    queue.enqueue("b", monotaskb4)
    val monotaskb5 = getMonotask(1)
    queue.enqueue("b", monotaskb5)
    val monotaskb6 = getMonotask(1)
    queue.enqueue("b", monotaskb6)
    val monotaskb7 = getMonotask(1)
    queue.enqueue("b", monotaskb7)

    // Items should be dequeued in round-robin fashion until a's queue is empty.
    assert(queue.dequeue() === monotaska1)
    assert(queue.dequeue() === monotaskb1)
    assert(queue.dequeue() === monotaska2)
    assert(queue.dequeue() === monotaskb2)
    // Now a's queue is empty, so all tasks for b should be run.
    assert(queue.dequeue() === monotaskb3)
    assert(queue.dequeue() === monotaskb4)

    // Add a new, big thing to the queue for a. a shouldn't have accumulated any deficit (since
    // its queue was empty), so it should need to wait for some of b's tasks to run first.
    val monotaska3 = getMonotask(3)
    queue.enqueue("a", monotaska3)

    assert(queue.dequeue() === monotaskb5)
    assert(queue.dequeue() === monotaskb6)
    assert(queue.dequeue() === monotaska3)
    assert(queue.dequeue() === monotaskb7)

  }
}

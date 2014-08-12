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

import java.util.Properties
import scala.collection.mutable.HashMap
import org.apache.spark.scheduler.Task

/**
 * A set of tasks submitted together to the low-level TaskScheduler, usually representing
 * missing partitions of a particular stage.
 */
private[spark] class TaskSet(
    val tasks: Array[Task[_]],
    val stageId: Int,
    val attempt: Int,
    val priority: Int,
    val properties: Properties) {
    val id: String = stageId + "." + attempt

  var depMap: Map[Task[_], Seq[Task[_]]] = tasks.map(x => (x, Seq[Task[_]]())).toMap

  def kill(interruptThread: Boolean) {
    tasks.foreach(_.kill(interruptThread))
  }

  override def toString: String = "TaskSet " + id
}

private[spark] object TaskSet {

  def setWithDeps(tasks: Array[Task[_]], depMap: HashMap[Task[_], Seq[Task[_]]],
                  stageId: Int, attempt: Int, priority: Int, properties: Properties) = {
    val s = new TaskSet(tasks, stageId, attempt, priority, properties)
    s.depMap = depMap.toMap
    s
  }

}
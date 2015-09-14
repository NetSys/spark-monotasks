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

package org.apache.spark.scheduler.cluster

import akka.actor.{Address, ActorRef}

/**
 * Grouping of data for an executor used by CoarseGrainedSchedulerBackend.
 *
 * @param executorActor The ActorRef representing this executor
 * @param executorAddress The network address of this executor
 * @param executorHost The hostname that this executor is running on
 * @param totalDisks The total number of disks available to the executor
 * @param totalCores The total number of cores available to the executor
 */
private[cluster] class ExecutorData(
   val executorActor: ActorRef,
   val executorAddress: Address,
   override val executorHost: String,
   val totalDisks: Int,
   override val totalCores: Int,
   override val logUrlMap: Map[String, String]
) extends ExecutorInfo(executorHost, totalCores, logUrlMap) {
  // Each executor can run at most (# cores) + (# disks) + (# network slots = 1) monotasks
  // concurrently, so cap the number of concurrent macrotasks per executor at this value.
  var freeSlots = totalCores + totalDisks + 1
}

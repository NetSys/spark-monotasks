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

package org.apache.spark.performance_logging

import scala.io.Source

case class NetworkCounters() extends Serializable {
  val timeMillis = System.currentTimeMillis()
  var receivedBytes = 0L
  var receivedPackets = 0L
  var transmittedBytes = 0L
  var transmittedPackets = 0L
  // Could also read per-process counters from s"/proc/${Utils.getPid()}/net/dev", but (a) this
  // doesn't work on m2.4xlarge instances (it's just the same as the total counters) and (b) if it
  // did work, it wouldn't include the HDFS network data (because that happens in a separate
  // process) which can be important to understanding utilization.
  Source.fromFile(s"/proc/net/dev").getLines().foreach { line =>
    if (line.contains(":") && !line.contains("lo")) {
      val counts = line.split(":")(1).split(" ").filter(_.length > 0).map(_.toLong)
      receivedBytes += counts(NetworkCounters.RECEIVED_BYTES_INDEX)
      receivedPackets += counts(NetworkCounters.RECEIVED_PACKETS_INDEX)
      transmittedBytes += counts(NetworkCounters.TRANSMITTED_BYTES_INDEX)
      transmittedPackets += counts(NetworkCounters.TRANSMITTED_PACKETS_INDEX)
    }
  }
}

object NetworkCounters {
  // 0-based index within the list of numbers in /proc/pid/net/dev file of the received and
  // transmitted bytes/packets. Not necessarily portable.
  val RECEIVED_BYTES_INDEX = 0
  val RECEIVED_PACKETS_INDEX = 1
  val TRANSMITTED_BYTES_INDEX = 8
  val TRANSMITTED_PACKETS_INDEX = 9
}

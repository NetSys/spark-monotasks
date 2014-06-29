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

class NetworkUtilization(startCounters: NetworkCounters, endCounters: NetworkCounters)
    extends Serializable {
  val elapsedMillis = endCounters.timeMillis - startCounters.timeMillis

  val bytesReceivedPerSecond =
    (endCounters.receivedBytes - startCounters.receivedBytes).toFloat * 1000 / elapsedMillis
  val bytesTransmittedPerSecond =
    (endCounters.transmittedBytes - startCounters.transmittedBytes).toFloat * 1000 / elapsedMillis
  val packetsReceivedPerSecond =
    (endCounters.receivedPackets - startCounters.receivedPackets).toFloat * 1000 / elapsedMillis
  val packetsTransmittedPerSecond =
    ((endCounters.transmittedPackets - startCounters.transmittedPackets).toFloat * 1000 /
      elapsedMillis)

  def this(startCounters: NetworkCounters) = this(startCounters, new NetworkCounters())
}

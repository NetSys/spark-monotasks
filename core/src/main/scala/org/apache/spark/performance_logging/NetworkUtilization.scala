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

package org.apache.spark.performance_logging

class NetworkUtilization(
    val startCounters: NetworkCounters,
    val endCounters: NetworkCounters)
  extends Serializable {

  def elapsedMillis: Long = endCounters.timeMillis - startCounters.timeMillis

  def bytesReceivedPerSecond: Double =
    (endCounters.receivedBytes - startCounters.receivedBytes).toDouble * 1000 / elapsedMillis

  def bytesTransmittedPerSecond: Double =
    (endCounters.transmittedBytes - startCounters.transmittedBytes).toDouble * 1000 / elapsedMillis

  def packetsReceivedPerSecond: Double =
    (endCounters.receivedPackets - startCounters.receivedPackets).toDouble * 1000 / elapsedMillis

  def packetsTransmittedPerSecond: Double =
    (endCounters.transmittedPackets - startCounters.transmittedPackets).toDouble * 1000 /
      elapsedMillis

  def this(startCounters: NetworkCounters) = {
    this(startCounters, new NetworkCounters())
  }
}

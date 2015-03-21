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
    val elapsedMillis: Long,
    val bytesReceivedPerSecond: Float,
    val bytesTransmittedPerSecond: Float,
    val packetsReceivedPerSecond: Float,
    val packetsTransmittedPerSecond: Float)
  extends Serializable {

  /**
   * This constructor is private because it is used only by other constructors, so they can
   * re-use elapsedMillis for all of the throughput calculations.
   */
  private def this(
      startCounters: NetworkCounters,
      endCounters: NetworkCounters,
      elapsedMillis: Long) = {
    this(
      elapsedMillis,
      (endCounters.receivedBytes - startCounters.receivedBytes).toFloat * 1000 / elapsedMillis,
      ((endCounters.transmittedBytes - startCounters.transmittedBytes).toFloat * 1000 /
        elapsedMillis),
      (endCounters.receivedPackets - startCounters.receivedPackets).toFloat * 1000 / elapsedMillis,
      ((endCounters.transmittedPackets - startCounters.transmittedPackets).toFloat * 1000 /
        elapsedMillis))
  }

  def this(startCounters: NetworkCounters, endCounters: NetworkCounters) = {
    this(startCounters, endCounters, endCounters.timeMillis - startCounters.timeMillis)
  }

  def this(startCounters: NetworkCounters) = {
    this(startCounters, new NetworkCounters())
  }
}

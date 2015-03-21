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

import scala.collection.mutable.HashMap

import org.scalatest.FunSuite

class DiskUtilizationSuite extends FunSuite {
  test("multiple block devices") {
    val startSectorsReadDisk1 = 100
    val startSectorsWrittenDisk1 = 10
    val startMillisReadingDisk1 = 1024
    val startMillisWritingDisk1 = 512
    val startMillisTotalDisk1 = 1536
    val disk1StartLine = s" 202      16 disk1 471 94 $startSectorsReadDisk1 "+
      s"$startMillisReadingDisk1 40 62 $startSectorsWrittenDisk1 $startMillisWritingDisk1 13 " +
      s"$startMillisTotalDisk1 0 6"

    val startSectorsReadDisk2 = 200
    val startSectorsWrittenDisk2 = 100
    val startMillisReadingDisk2 = 512
    val startMillisWritingDisk2 = 512
    val startMillisTotalDisk2 = 1024
    // Intentionally use different spacing to make sure things still work properly.
    val disk2StartLine = s" 202      16 disk2 471 94 $startSectorsReadDisk2 " +
      s"$startMillisReadingDisk2 40 62 $startSectorsWrittenDisk2 $startMillisWritingDisk2 13 " +
      s"$startMillisTotalDisk2 0 6"

    val startNameToCounters = HashMap[String, BlockDeviceCounters]()
    startNameToCounters += "disk1" -> new BlockDeviceCounters(disk1StartLine)
    startNameToCounters += "disk2" -> new BlockDeviceCounters(disk2StartLine)

    val startCounters = new DiskCounters(0, startNameToCounters)

    val endSectorsReadDisk1 = 100
    val endSectorsWrittenDisk1 = 20
    val endMillisReadingDisk1 = 1024
    val endMillisWritingDisk1 = 1024
    val endMillisTotalDisk1 = 2048
    val disk1EndLine = s" 202      16 disk1 471 94 $endSectorsReadDisk1 "+
      s"$endMillisReadingDisk1 40 62 $endSectorsWrittenDisk1 $endMillisWritingDisk1 13 " +
      s"$endMillisTotalDisk1 0 6"

    val endSectorsReadDisk2 = 400
    val endSectorsWrittenDisk2 = 300
    val endMillisReadingDisk2 = 1024
    val endMillisWritingDisk2 = 1024
    val endMillisTotalDisk2 = 2048
    // Intentionally use different spacing to make sure things still work properly.
    val disk2EndLine = s" 202      16 disk2 471 94 $endSectorsReadDisk2 " +
      s"$endMillisReadingDisk2 40 62 $endSectorsWrittenDisk2 $endMillisWritingDisk2 13 " +
      s"$endMillisTotalDisk2 0 6"

    val endNameToCounters = HashMap[String, BlockDeviceCounters]()
    endNameToCounters += "disk1" -> new BlockDeviceCounters(disk1EndLine)
    endNameToCounters += "disk2" -> new BlockDeviceCounters(disk2EndLine)

    val endCounters = new DiskCounters(2048, endNameToCounters)

    val utilization = DiskUtilization(startCounters, endCounters)
    assert(utilization.deviceNameToUtilization.contains("disk1"))
    val disk1Utilization = utilization.deviceNameToUtilization("disk1")
    assert(0 === disk1Utilization.readThroughput)
    // 10 sectors written * 512 bytes / sector * 1000 ms / second / 2048 ms = 2500 bytes / s
    assert(2500 === disk1Utilization.writeThroughput)
    assert(0.25 === disk1Utilization.diskUtilization)

    assert(utilization.deviceNameToUtilization.contains("disk2"))
    val disk2Utilization = utilization.deviceNameToUtilization("disk2")
    // 200 sectors * 512 bytes / sector * 1000 ms / second / 2048 ms = 50000 bytes / s
    assert(50000 === disk2Utilization.readThroughput)
    assert(50000 === disk2Utilization.writeThroughput)
    assert(0.5 === disk2Utilization.diskUtilization)
  }
}

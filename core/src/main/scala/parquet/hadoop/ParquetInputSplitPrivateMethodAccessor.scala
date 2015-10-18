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

package parquet.hadoop

import java.util.Map

/** This class is a hack used to access package-private methods in ParquetInputSplit. */
class ParquetInputSplitPrivateMethodAccessor(private val sourceSplit: ParquetInputSplit) {

  def getEnd(): Long = sourceSplit.getEnd()
  def getRowGroupOffsets(): Array[Long] = sourceSplit.getRowGroupOffsets()
  def getRequestedSchema(): String = sourceSplit.getRequestedSchema()
  def getReadSupportMetadata(): Map[String, String] = sourceSplit.getReadSupportMetadata()
}

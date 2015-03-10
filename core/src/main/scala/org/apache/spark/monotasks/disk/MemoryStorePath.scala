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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.storage.BlockId

/**
 * A subclass of Path that makes it possible for Path.getFileSystem() to return a
 * MemoryStoreFileSystem. The blockId is used by MemoryStoreFileSystem to determine which block to
 * read from the MemoryStore.
 */
class MemoryStorePath(
    underlyingHadoopPath: URI,
    val blockId: BlockId,
    fileSystem: MemoryStoreFileSystem)
  extends Path(underlyingHadoopPath) {

  override def getFileSystem(conf: Configuration): FileSystem =
    fileSystem
}

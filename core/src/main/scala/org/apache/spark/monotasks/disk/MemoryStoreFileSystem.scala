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

import java.io.EOFException
import java.net.URI
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.Logging
import org.apache.spark.storage.{BlockException, BlockManager}
import org.apache.spark.util.ByteBufferInputStream

/**
 * This is a Hadoop FileSystem that only supports one operation: opening files by fetching blocks
 * from the BlockManager. All methods that read large amounts of data from disk are not supported,
 * except for `open()`, which retrieves data from the BlockManager.
 *
 * Methods that fetch file metadata or system information delegate to `underlyingFileSystem`. Note
 * that some such methods (`getFileStatus()` and `listStatus()` in particular) may fetch a small
 * amount of data from disk.
 */
class MemoryStoreFileSystem(
    private val blockManager: BlockManager,
    private val startPosition: Long,
    private val underlyingFileSystem: FileSystem,
    hadoopConf: Configuration)
  extends FileSystem with Logging {

  setConf(hadoopConf)

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    throwError()

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    throwError()

  override def delete(f: Path, recursive: Boolean): Boolean =
    throwError()

  override def delete(f: Path): Boolean =
    throwError()

  override def getFileStatus(f: Path): FileStatus =
    underlyingFileSystem.getFileStatus(f)

  override def getUri(): URI =
    underlyingFileSystem.getUri()

  override def getWorkingDirectory(): Path =
    underlyingFileSystem.getWorkingDirectory()

  override def listStatus(f: Path): Array[FileStatus] =
    underlyingFileSystem.listStatus(f)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    throwError()

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val blockId = f match {
      case memoryPath: MemoryStorePath =>
        memoryPath.blockId
      case path: Any =>
        throw new UnsupportedOperationException("Unsupported Path: $path. " +
          "This FileSystem only supports opening MemoryStorePaths.")
    }
    blockManager.getLocalBytes(blockId).map { buffer =>
      new FSDataInputStream(new ByteBufferFSDataInputStream(buffer, startPosition))
    }.getOrElse {
      val message = s"Block $blockId not found in the BlockManager."
      logError(message)
      throw new BlockException(blockId, message)
    }
  }

  override def rename(src: Path, dst: Path): Boolean =
    throwError()

  override def setWorkingDirectory(new_dir: Path): Unit =
    underlyingFileSystem.setWorkingDirectory(new_dir)

  private def throwError() =
    throw new UnsupportedOperationException(
      "The only operation that this FileSystem supports is opening splits that are stored " +
        "in the BlockManager.")
}

/**
 * This class is used to create an FSDataInputStream that reads from a ByteBuffer. We cannot use the
 * ByteBufferInputStream class directly because the InputStream used to create an FSDataInputStream
 * must implement the PositionedReadable and Seekable interfaces. This class is simply a wrapper for
 * ByteBufferInputStream that implemenents the required interfaces.
 */
private class ByteBufferFSDataInputStream(buffer: ByteBuffer, private val startPosition: Long)
  extends ByteBufferInputStream(buffer, false) with PositionedReadable with Seekable {

  def read(position: Long, dst: Array[Byte], offset: Int, length: Int): Int = {
    val newPosition = (position - startPosition).toInt
    val newPositionBuffer = buffer.duplicate().position(newPosition).asInstanceOf[ByteBuffer]
    new ByteBufferInputStream(newPositionBuffer, false).read(dst, offset, length)
  }

  def readFully(position: Long, dst: Array[Byte]): Unit = {
    readFully(position, dst, 0, dst.length)
  }

  def readFully(position: Long, dst: Array[Byte], offset: Int, length: Int): Unit = {
    if ((position - startPosition) + length >= buffer.limit()) {
      throw new EOFException("Reached end of file while reading.")
    }
    read(position, dst, offset, length)
  }

  def getPos(): Long =
    buffer.position() + startPosition

  def seek(pos: Long) =
    buffer.position((pos - startPosition).toInt)

  def seekToNewSource(targetPos: Long): Boolean =
    throw new UnsupportedOperationException("Cannot seek to a new source.")
}

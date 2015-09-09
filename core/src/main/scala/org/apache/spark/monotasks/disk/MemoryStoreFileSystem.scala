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

import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.Logging
import org.apache.spark.storage.{BlockException, BlockManager}
import org.apache.spark.util.{ByteArrayOutputStreamWithZeroCopyByteBuffer, ByteBufferInputStream}

/**
 * This is a Hadoop FileSystem that only supports two operations, `create()` and `open()`, which
 * both deal only with files that are stored in memory.
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

  /** The in-memory output streams that represent files that were created using this FileSystem. */
  private val fileBuffers = new HashMap[Path, ByteArrayOutputStreamWithZeroCopyByteBuffer]()

  /**
   * Returns a ByteBuffer containing the data that has been written to the specified file so far.
   * This method removes the specified path from the MemoryStoreFileSystem's internal data
   * structures, meaning that it can only be called once per file.
   */
  def getFileByteBuffer(path: Path): ByteBuffer =
    fileBuffers.remove(path).map(_.getByteBuffer()).getOrElse(
      throw new UnsupportedOperationException(s"File $path was not created using this instance " +
        s"of ${classOf[MemoryStoreFileSystem].getName()}."))

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    throwError()

  /**
   * Returns an FSDataOutputStream that writes to an in-memory buffer. All input parameters except
   * `path` are ignored. `path` must be an instance of
   * `org.apache.spark.monotasks.disk.MemoryStorePath`.
   *
   * TODO: Although not required at this time, we may eventually need to devise a way to pass the
   *       other input parameters to the HdfsWriteMonotask that will actually write this file to
   *       disk so that it can use them when creating an FSDataOutputStream.
   */
  override def create(
      path: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {

    if (!path.isInstanceOf[MemoryStorePath]) {
      throw new IllegalArgumentException("MemoryStoreFileSystem.create() only supports paths of " +
        s"type ${classOf[MemoryStorePath].getName()}, but was called " +
        s"with a ${path.getClass().getName()}")
    }

    val buffer = new ByteArrayOutputStreamWithZeroCopyByteBuffer()
    fileBuffers(path) = buffer

    logDebug(s"Creating file $path, which is backed by an in-memory buffer.")
    new FSDataOutputStream(buffer)
  }

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

  /**
   * Assuming that `path` is a MemoryStorePath, this method opens the file that is stored in the
   * MemoryStore using `path.blockId`.
   */
  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    val blockId = path match {
      case memoryPath: MemoryStorePath =>
        memoryPath.blockId.getOrElse(
          throw new IllegalStateException(
           s"MemoryStorePath $memoryPath does not specify a BlockId to use to fetch the file " +
           "from the MemoryStore."))
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
      "This MemoryStoreFileSystem only supports create(), open(), and file metadata queries.")
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

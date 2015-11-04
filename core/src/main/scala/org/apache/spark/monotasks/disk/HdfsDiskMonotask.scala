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

import java.net.InetAddress
import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.util.Random

import org.apache.commons.codec.binary.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, VolumeId}
import org.apache.hadoop.hdfs.{DFSConfigKeys, DistributedFileSystem}

import org.apache.spark.{SparkException, TaskContextImpl}
import org.apache.spark.storage.{BlockFileManager, BlockId}

/** This is the superclass of all DiskMonotasks that operate on HDFS files. */
private[spark] abstract class HdfsDiskMonotask(
    taskContext: TaskContextImpl,
    blockId: BlockId,
    private val hadoopConf: Configuration)
  extends DiskMonotask(taskContext, blockId) {

  /** The path to the HDFS file that this HdfsDiskMonotask will access. */
  protected var pathOpt: Option[Path] = None

  protected def getPath(): Path =
    pathOpt.getOrElse(
      throw new UnsupportedOperationException("This HdfsDiskMonotask's path has not been set."))

  /**
   * Returns the Spark local directory that is located on the same physical disk as the HDFS path
   * that this monotask will access.
   *
   * @param sparkLocalDirs The Spark local directories. One of these will be returned. This is
   *                       assumed to be non-empty.
   */
  def chooseLocalDir(sparkLocalDirs: Seq[String]): String = {
    val path = getPath()
    val pathString = hdfsPathToString(path)
    val localPathOpt = path.getFileSystem(hadoopConf) match {
      case dfs: DistributedFileSystem =>
        // The path resides in the Hadoop Distributed File System. We need to do extra work to
        // figure out if the file is stored locally. If the file is stored locally, then we will
        // determine the local filesystem directory that contains it. This is accomplished by
        // querying the Hadoop Distributed FileSystem for the storage locations of the file,
        // checking if any of the storage locations are local, and if so using the local storage
        // location's VolumeId to determine which of the HDFS data directories holds the file.
        logDebug(s"Block $blockId is located on a distributed filesystem.")
        findLocalDirectory(path, dfs)

      case _: FileSystem =>
        // The path resides on the local filesystem.
        logDebug(s"Block $blockId is located on a local filesystem.")
        Some(pathString)
    }

    val chosenLocalDirIndex = localPathOpt.flatMap { localPath =>
      logDebug(s"The local path for block $blockId is: $localPath")

      // Find a Spark local directory that is on the same disk as the HDFS data directory.
      val dataDiskName = BlockFileManager.getDiskNameFromPath(localPath)
      val localDirsDiskNames = sparkLocalDirs.map(BlockFileManager.getDiskNameFromPath)
      val localDirIndexOpt = localDirsDiskNames.zipWithIndex.find(_._1 == dataDiskName).map(_._2)

      if (localDirIndexOpt.isEmpty) {
        logWarning(s"Block $blockId (on disk $dataDiskName) is not stored on the same disk as " +
          "any of the Spark local directories (which are on disks: " +
          s"${localDirsDiskNames.mkString("[", ", ", "]")}). Falling back to choosing a " +
          "Spark local directory at random.")
      }

      localDirIndexOpt
    }.getOrElse {
      logDebug(s"Unable to find the local path for block $blockId. Selecting a Spark local " +
        "directory at random.")
      Random.nextInt(sparkLocalDirs.size)
    }

    val chosenLocalDir = sparkLocalDirs(chosenLocalDirIndex)
    logDebug(s"Choosing Spark local directory $chosenLocalDir for HDFS file $pathString " +
      s"(blockId: $blockId).")
    chosenLocalDir
  }

  /**
   * Returns the HDFS path specified by `path`, with the hostname and port information removed.
   *
   * @param path The path to an HDFS file.
   */
  private def hdfsPathToString(path: Path): String = path.toUri().getPath()

  /**
   * Queries the provided DistributedFileSystem to determine if the file specified by `path` is
   * stored locally. If it is, then this method returns the path to the HDFS data directory in which
   * the file is stored. If the file is empty, is not stored locally, or an error occurs, then this
   * method returns None.
   *
   * @param path The path to an HDFS file.
   * @param dfs The DistributedFileSystem object that is the interface to HDFS.
   */
  private def findLocalDirectory(path: Path, dfs: DistributedFileSystem): Option[String] = {
    val sizeBytes = dfs.getFileStatus(path).getLen()

    // dfs.getFileBlockLocations() returns BlockLocation objects for all of the Hadoop blocks that
    // make up the file specified by path. The Monotasks design specifies that each RDD partition
    // corresponds to at most one HDFS block, so this call should return at most one
    // BlockStorageLocation object.
    val locationsPerBlock = dfs.getFileBlockLocations(path, 0, sizeBytes).toSeq
    val numBlocks = locationsPerBlock.size
    val pathString = hdfsPathToString(path)
    if (numBlocks == 0) {
      logDebug(s"HDFS file $pathString is empty.")
      return None
    } else if (numBlocks != 1) {
      throw new SparkException(s"HDFS file $pathString is comprised of $numBlocks blocks. " +
        "Currently, only empty HDFS files and HDFS files consisting of one block are supported.")
    }
    val storageLocations = dfs.getFileBlockStorageLocations(locationsPerBlock).head

    // The block may be stored (replicated) on multiple machines.
    val ipAddresses = storageLocations.getNames().map(_.split(":").head)
    val volumeIds = storageLocations.getVolumeIds()
    if (ipAddresses.size != volumeIds.size) {
      logWarning(s"IP addresses ($ipAddresses) and VolumeIds ($volumeIds) corresponding to block " +
        "locations are expected to be the same size.")
      return None
    }

    val localIpAddress = InetAddress.getLocalHost().getHostAddress()
    val localVolumeIds = ipAddresses.zip(volumeIds).filter(_._1 == localIpAddress).map(_._2)
    val localDirectory = localVolumeIds.headOption.flatMap(extractDataDir)
    if (localDirectory.isEmpty) {
      logWarning(s"No HDFS blocks for path $pathString are stored locally.")
    }
    localDirectory
  }

  /**
   * This method parses `volumeId` to determine which HDFS data directory it corresponds to, and
   * returns the path to that directory or None if an error occurs.
   *
   * @param volumeId An encoded identifier that corresponds to a local data directory.
   */
  private def extractDataDir(volumeId: VolumeId): Option[String] = {
    // VolumeId.toString() returns the data directory index encoded in base 64.
    //
    // TODO: This is a hack and eventually we will need to address this by using a public API. Also,
    //       we need to be very mindful of this when we change the version of HDFS. This only works
    //       for Hadoop 2.0.3 - 2.3, inclusive. The VolumeId class was introduced in 2.0.2, but did
    //       not start using base 64 encoding until 2.0.3. In version 2.4.0, VolumeId.toString() is
    //       refactored to return the data directory index encoded in hex, instead of in base 64
    //       (see HDFS-3969: https://issues.apache.org/jira/browse/HDFS-3969).
    val volumeIdString = volumeId.toString()
    val volumeIdBytes = Base64.decodeBase64(volumeIdString)
    val length = volumeIdBytes.length
    if (length != 4) {
      logWarning(s"The byte array representation of VolumeId $volumeIdString has length $length, " +
        "whereas the expected length is 4 (so that it can be converted to an Int).")
      return None
    }

    val index = ByteBuffer.wrap(volumeIdBytes).getInt
    val dataDirs = hadoopConf.getTrimmedStringCollection(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY)
    val numDataDirs = dataDirs.size
    if ((index < 0) || (index >= numDataDirs)) {
      logError(s"Disk index for block $blockId is out of bounds " +
        s"(it is $index when it should be in the range (0, ${numDataDirs - 1})).")
      return None
    }

    Some(dataDirs.toSeq.get(index))
  }
}

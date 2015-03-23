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
import scala.reflect.classTag
import scala.util.Random

import org.apache.commons.codec.binary.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, VolumeId}
import org.apache.hadoop.hdfs.{DFSConfigKeys, DistributedFileSystem}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import org.apache.spark.{Partition, SparkEnv, SparkException, TaskContextImpl}
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.rdd.NewHadoopPartition
import org.apache.spark.storage.{BlockFileManager, MonotaskResultBlockId, RDDBlockId, StorageLevel}

/** Contains the parameters and logic necessary to read an HDFS file and cache it in memory. */
private[spark] class HdfsReadMonotask(
    sparkTaskContext: TaskContextImpl,
    rddId: Int,
    sparkPartition: Partition,
    private val hadoopConf: Configuration)
  extends HdfsDiskMonotask(sparkTaskContext, new RDDBlockId(rddId, sparkPartition.index)) {

  private val hadoopSplit =
    sparkPartition.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value match {
      case fileSplit: FileSplit =>
        fileSplit
      case otherSplit =>
        throw new UnsupportedOperationException("Unsupported InputSplit: " +
          s"${otherSplit.getClass().getName()}. HdfsReadMonotask only supports InputSplits of " +
          s"type ${classTag[FileSplit]}.")
    }

  private val path = hadoopSplit.getPath()
  resultBlockId = Some(new MonotaskResultBlockId(taskId))

  override def execute(): Unit = {
    val stream = path.getFileSystem(hadoopConf).open(path)
    val numBytes = hadoopSplit.getLength().toInt
    val buffer = new Array[Byte](numBytes)

    try {
      stream.readFully(hadoopSplit.getStart(), buffer)
      SparkEnv.get.blockManager.cacheBytes(
        getResultBlockId(), ByteBuffer.wrap(buffer), StorageLevel.MEMORY_ONLY_SER, false)
    } finally {
      stream.close()
    }

    context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Hadoop).incBytesRead(numBytes)
  }

  override def chooseLocalDir(sparkLocalDirs: Seq[String]): String = {
    val pathString = hdfsPathToString(path)
    val localPath = path.getFileSystem(hadoopConf) match {
      case dfs: DistributedFileSystem =>
        // The path resides in the Hadoop Distributed File System. We need to do extra work to
        // figure out if the file is stored locally. If the file is stored locally, then we will
        // determine the local filesystem directory that contains it. This is accomplished by
        // querying the Hadoop Distributed FileSystem for the storage locations of the file,
        // checking if any of the storage locations are local, and if so using the local storage
        // location's VolumeId to determine which of the HDFS data directories holds the file.
        //
        // TODO: Eventually, we may want to remove the restriction that a local data directory must
        //       be found.
        findLocalDirectory(path, dfs).getOrElse(
          throw new SparkException("Unable to determine which HDFS data directory the block " +
            s"$blockId is stored in."))

      case _: FileSystem =>
        // The path resides on the local filesystem.
        pathString
    }

    // Find a Spark local directory that is on the same disk as the HDFS data directory.
    val dataDiskId = BlockFileManager.pathToDiskId(localPath)
    val chosenLocalDir = {
      val localDirsDiskIds = sparkLocalDirs.map(BlockFileManager.pathToDiskId)
      val localDirIndex =
        localDirsDiskIds.zipWithIndex.find(_._1 == dataDiskId).map(_._2).getOrElse {
          logWarning(s"Block $blockId (on disk $dataDiskId) is not stored on the same disk as " +
            "any of the Spark local directories (which are on disks: " +
            s"${localDirsDiskIds.mkString("[", ", ", "]")}). Falling back to choosing a " +
            "Spark local directory at random.")
          Random.nextInt(sparkLocalDirs.size)
        }
      sparkLocalDirs(localDirIndex)
    }

    logDebug(s"Choosing Spark local directory $chosenLocalDir for HDFS file $pathString " +
      s"(blockId: $blockId).")
    chosenLocalDir
  }

    /**
   * Returns the raw HDFS path specified by `path`, with the hostname and port information removed.
   *
   * @param path The path to an HDFS file.
   */
  private def hdfsPathToString(path: Path): String = path.toUri().getPath()

  /**
   * Queries the provided DistributedFileSystem to determine if the file specified by `path` is
   * stored locally. If it is, then this method returns the path to the HDFS data directory in which
   * the file is stored. If the file is not stored locally or an error occurs, this method returns
   * None.
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
    if (numBlocks != 1) {
      throw new SparkException(s"HDFS file $pathString is comprised of $numBlocks blocks. " +
        "Currently, only HDFS files consisting of one block are supported.")
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

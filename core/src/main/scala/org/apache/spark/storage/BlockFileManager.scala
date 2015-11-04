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

package org.apache.spark.storage

import java.io.File
import java.nio.file.Files
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import scala.collection.mutable.HashMap

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.Utils

/**
 * Creates and maintains the mapping between logical blocks and physical on-disk locations
 *
 * Typically, one block is mapped to one file with a name given by its BlockId. For shuffle blocks,
 * all blocks for the same shuffle and for the same map task are saved to a single file.
 *
 * Block files are divided among the directories listed in spark.local.dir (or in SPARK_LOCAL_DIRS,
 * if it is set), which will be referred to as top-level directories. We assume that each top-level
 * directory resides on a separate physical disk; violating this assumption will hurt performance.
 * While the BlockFileManager keeps track of the top-level directories, it does not actually decide
 * which top-level directory a block maps to. That task is the responsibility of whoever uses the
 * BlockFileManager. In order to avoid creating large inodes, the BlockFileManager creates a number
 * of sub-directories in each top-level directory and, given a top-level directory, has the
 * responsibility of managing where a block is located its sub-directory structure.
 */
private[spark] class BlockFileManager(conf: SparkConf) extends Logging {

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  private val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)

  /* Mapping from disk identifier to spark local directory, for each path mentioned in
   * spark.local.dir */
  val localDirs = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.BLOCK_FILE_MANAGER_FAILED_TO_CREATE_DIR)
  }
  /* Mapping from disk identifier to a list of sub-directories that exist inside of the disk
   * identifier's corresponding entry in localDirs. Sub-directories are only created when they are
   * used, and are initially all null. This system of sub-directories is used to avoid having really
   * large inodes at the top level. */
  private val subDirs = createSubDirs()

  def getFile(filename: String, diskId: String): Option[File] = {
    if (!localDirs.contains(diskId) || !subDirs.contains(diskId)) {
      logError(s"Unable to retrieve file due to invalid diskId: $diskId")
      return None
    }
    // Figure out which sub-directory the filename hashes to.
    val subDirId = (Utils.nonNegativeHash(filename) / localDirs.size) % subDirsPerLocalDir
    val subDirList = subDirs(diskId)
    var subDir = subDirList(subDirId)
    // Create the sub-directory if it doesn't already exist.
    if (subDir == null) {
      /* Prevent multiple threads from creating the same sub-directory at the same time. Synchronize
       * here instead of when first accessing subDirList to avoid unnecessary synchronization if
       * subDir does not need to be created. */
      subDir = subDirList.synchronized {
        val old = subDirList(subDirId)
        if (old != null) {
          old
        } else {
          val newDir = new File(localDirs(diskId), "%02x".format(subDirId))
          newDir.mkdir()
          subDirList(subDirId) = newDir
          newDir
        }
      }
    }
    Some(new File(subDir, filename))
  }

  def getBlockFile(blockId: BlockId, diskId: String): Option[File] = {
    val filename = blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        // All shuffle blocks for a map task in a given shuffle are stored in the same file.
        MultipleShuffleBlocksId(shuffleId, mapId).name

      case _ =>
        blockId.name
    }
    getFile(filename, diskId)
  }

  /** Checks if a file for the specified block exists on the specified disk. */
  def contains(blockId: BlockId, diskId: Option[String]): Boolean =
    diskId.map(getBlockFile(blockId, _).map(_.exists()).getOrElse(false)).getOrElse(false)

  /** Lists all files currently stored on all disks by this BlockFileManager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of sub-directory arrays.
    subDirs.values.toSeq.flatten.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** Lists all blocks currently stored on all disks by this BlockFileManager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().map(f => BlockId(f.getName))
  }

  /**
   * Returns the size in bytes of the file corresponding to the specified block, if it exists on
   * the specified disk.
   */
  def getSize(blockId: BlockId, diskId: String): Long = {
    getBlockFile(blockId, diskId).map(_.length).getOrElse(0L)
  }

  private def createLocalDirs(conf: SparkConf): HashMap[String, File] = {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    // Create local directory files.
    val localDirs = new HashMap[String, File]()
    Utils.getOrCreateLocalRootDirs(conf).foreach { rootDir =>
      var foundLocalDir = false
      var localDir: File = null
      var localDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          localDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          localDir = new File(rootDir, s"spark-local-$localDirId")
          if (!localDir.exists) {
            foundLocalDir = localDir.mkdirs()
          }
        } catch {
          case e: Exception =>
            logWarning(s"Attempt $tries to create local dir $localDir failed", e)
        }
      }
      if (foundLocalDir) {
        logInfo(s"Created local directory at $localDir")
        /* For disk scheduling purposes, each Spark local directory is considered to be a separate
         * physical disk. In reality, this does not have to be the case. */
        localDirs(localDir.toPath().toString()) = localDir

        // Register this local directory to be automatically deleted during shutdown.
        Utils.registerShutdownDeleteDir(localDir)
      } else {
        logError(s"Failed $MAX_DIR_CREATION_ATTEMPTS attempts to create local dir in $rootDir. " +
          "Ignoring this directory.")
      }
    }
    val localDirsPaths = localDirs.keys.toArray
    val diskNames = localDirsPaths.map(BlockFileManager.getDiskNameFromPath)
    // Since Sets do not contain duplicates, if diskNames.toSet contains more elements than
    // diskNames, then diskNames must contain duplicates, which implies that multiple Spark local
    // directories reside on the same physical disk. Having multiple Spark local directories on the
    // same physical disk can hurt DiskScheduler performance.
    if (diskNames.size != diskNames.toSet.size) {
      val details = localDirsPaths.zip(diskNames).map(a => s"${a._1} : ${a._2}").mkString("\n")
      logWarning("Spark local directories do not reside on separate physical disks. For best " +
        "performance, each Spark local directory should reside on a separate physical disk." +
        s"\nLocal Directory : Disk\n$details")
    }
    localDirs
  }

  private def createSubDirs(): HashMap[String, Array[File]] = {
    val subDirs = new HashMap[String, Array[File]]()
    for ((diskId, _) <- localDirs) {
      subDirs(diskId) = new Array[File](subDirsPerLocalDir)
    }
    subDirs
  }
}

private[spark] object BlockFileManager {

  /** Returns the name of the physical disk on which the provided path is located. */
  def getDiskNameFromPath(path: String): String = Files.getFileStore(new File(path).toPath()).name()
}

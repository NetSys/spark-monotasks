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

import java.io.{BufferedOutputStream, ByteArrayOutputStream, File, InputStream, OutputStream}
import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

import akka.actor.{ActorSystem, Props}
import sun.nio.ch.DirectBuffer

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.disk.DiskReadMonotask
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.util._

private[spark] sealed trait BlockValues
private[spark] case class ByteBufferValues(buffer: ByteBuffer) extends BlockValues
private[spark] case class IteratorValues(iterator: Iterator[Any]) extends BlockValues
private[spark] case class ArrayValues(buffer: Array[Any]) extends BlockValues

/* Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
    val data: Iterator[Any],
    readMethod: DataReadMethod.Value,
    bytes: Long) {
  val inputMetrics = new InputMetrics(readMethod)
  inputMetrics.incBytesRead(bytes)
}

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that #initialize() must be called before the BlockManager is usable.
 */
private[spark] class BlockManager(
    executorId: String,
    actorSystem: ActorSystem,
    val master: BlockManagerMaster,
    defaultSerializer: Serializer,
    maxMemory: Long,
    val conf: SparkConf,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    private[spark] val blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int)
  extends BlockDataManager with Logging {

  val blockFileManager = new BlockFileManager(conf)

  private val blockInfo = new TimeStampedHashMap[BlockId, BlockInfo]

  // Actual storage of where blocks are kept
  private var tachyonInitialized = false
  private[spark] val memoryStore = new MemoryStore(this, maxMemory)
  private[spark] lazy val tachyonStore: TachyonStore = {
    val storeDir = conf.get("spark.tachyonStore.baseDir", "/tmp_spark_tachyon")
    val appFolderName = conf.get("spark.tachyonStore.folderName")
    val tachyonStorePath = s"$storeDir/$appFolderName/${this.executorId}"
    val tachyonMaster = conf.get("spark.tachyonStore.url",  "tachyon://localhost:19998")
    val tachyonBlockManager =
      new TachyonBlockManager(this, tachyonStorePath, tachyonMaster)
    tachyonInitialized = true
    new TachyonStore(this, tachyonBlockManager)
  }

  var blockManagerId: BlockManagerId = _

  // Whether to compress broadcast variables that are stored
  private val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  // Whether to compress shuffle output that are stored
  private val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // Whether to compress RDD partitions that are stored serialized
  private val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  private val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

  private val slaveActor = actorSystem.actorOf(
    Props(new BlockManagerSlaveActor(this, mapOutputTracker)),
    name = "BlockManagerActor" + BlockManager.ID_GENERATOR.next)

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  private val metadataCleaner = new MetadataCleaner(
    MetadataCleanerType.BLOCK_MANAGER, this.dropOldNonBroadcastBlocks, conf)
  private val broadcastCleaner = new MetadataCleaner(
    MetadataCleanerType.BROADCAST_VARS, this.dropOldBroadcastBlocks, conf)

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

  /**
   * Construct a BlockManager with a memory limit set based on system properties.
   */
  def this(
      execId: String,
      actorSystem: ActorSystem,
      master: BlockManagerMaster,
      serializer: Serializer,
      conf: SparkConf,
      mapOutputTracker: MapOutputTracker,
      shuffleManager: ShuffleManager,
      blockTransferService: BlockTransferService,
      securityManager: SecurityManager,
      numUsableCores: Int) = {
    this(execId, actorSystem, master, serializer, BlockManager.getMaxMemory(conf),
      conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager, numUsableCores)
  }

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster and starts the BlockManagerWorker actor.
   */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    blockTransferService.init(appId)

    blockManagerId = BlockManagerId(
      executorId, blockTransferService.hostName, blockTransferService.port)

    master.registerBlockManager(blockManagerId, maxMemory, slaveActor)
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfo.size} blocks to the master.")
    for ((blockId, info) <- blockInfo) {
      val status = getCurrentBlockStatus(blockId, info)
      if (!tryToReportBlockStatus(blockId, info, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo("BlockManager re-registering with master")
    master.registerBlockManager(blockManagerId, maxMemory, slaveActor)
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      Await.ready(task, Duration.Inf)
    }
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      shuffleManager.shuffleBlockManager.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      val blockBytesOpt = doGetLocal(blockId, asBlockResult = false)
        .asInstanceOf[Option[ByteBuffer]]
      if (blockBytesOpt.isDefined) {
        val buffer = blockBytesOpt.get
        new NioManagedBuffer(buffer)
      } else {
        throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   */
  override def cacheBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel): Unit = {
    cacheBytes(blockId, data.nioByteBuffer(), level)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing, and it doesn't fetch information from Tachyon.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfo.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskId = info.diskId
      val isOnDisk = blockFileManager.contains(blockId, info.diskId)
      val diskSize = if (isOnDisk) blockFileManager.getSize(blockId, diskId.get) else 0L
      // Assume that block is not in Tachyon
      BlockStatus(info.level, memSize, diskSize, 0L, info.diskId)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the BlockFileManager (that the BlockManager
   * may not know of).
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    (blockInfo.keys ++ blockFileManager.getAllBlocks()).filter(filter).toSeq
  }

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
   */
  private def reportBlockStatus(
      blockId: BlockId,
      info: BlockInfo,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    val needReregister = !tryToReportBlockStatus(blockId, info, status, droppedMemorySize)
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Tells the master about the current storage status of the specified block, if this BlockManager
   * knows about it. This is a wrapper for the reportBlockStatus() above.
   */
  private def reportBlockStatus(blockId: BlockId) {
    if (blockInfo.contains(blockId)) {
      val info = blockInfo(blockId)
      // Prevent concurrent access to a block's BlockInfo object.
      info.synchronized {
        val status = getCurrentBlockStatus(blockId, info)
        if (status.storageLevel != StorageLevel.NONE) {
          reportBlockStatus(blockId, info, status)
        }
      }
    }
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      info: BlockInfo,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    if (info.tellMaster) {
      val storageLevel = status.storageLevel
      val inMemSize = Math.max(status.memSize, droppedMemorySize)
      val inTachyonSize = status.tachyonSize
      val onDiskSize = status.diskSize
      master.updateBlockInfo(
        blockManagerId, blockId, storageLevel, inMemSize, onDiskSize, inTachyonSize, info.diskId)
    } else {
      true
    }
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus(StorageLevel.NONE, 0L, 0L, 0L, None)
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val inTachyon = level.useOffHeap && tachyonStore.contains(blockId)
          val diskId = info.diskId
          val onDisk = level.useDisk && blockFileManager.contains(blockId, diskId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem || inTachyon || onDisk) level.replication else 1
          val storageLevel = StorageLevel(onDisk, inMem, inTachyon, deserialized, replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val tachyonSize = if (inTachyon) tachyonStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) blockFileManager.getSize(blockId, diskId.get) else 0L
          BlockStatus(storageLevel, memSize, diskSize, tachyonSize, diskId)
      }
    }
  }

  /**
   * Returns the current status of the specified block, or None if the BlockManager does not know
   * about the block.
   */
  def getCurrentBlockStatus(blockId: BlockId): Option[BlockStatus] = {
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        Some(getCurrentBlockStatus(blockId, info))
      }
    } else {
      None
    }
  }

  /**
   * Get locations of an array of blocks.
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
   * Get block from local block manager.
   */
  def getLocal(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    doGetLocal(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockManager = shuffleManager.shuffleBlockManager
      shuffleBlockManager.getBytes(blockId.asInstanceOf[ShuffleBlockId]) match {
        case Some(bytes) =>
          Some(bytes)
        case None =>
          throw new BlockException(
            blockId, s"Block $blockId not found on disk, though it should be")
      }
    } else {
      doGetLocal(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
    }
  }

  private def doGetLocal(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        // Double check to make sure the block is still there. There is a small chance that the
        // block has been removed by removeBlock (which also synchronizes on the blockInfo object).
        // Note that this only checks metadata tracking. If user intentionally deleted the block
        // on disk or from off heap storage without using removeBlock, this conditional check will
        // still pass but eventually we will get an exception because we can't find the block.
        if (blockInfo.get(blockId).isEmpty) {
          logWarning(s"Block $blockId had been removed")
          return None
        }

        // If another thread is writing the block, wait for it to become ready.
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure.")
          return None
        }

        val level = info.level
        logDebug(s"Level for block $blockId is $level")

        // Look for the block in memory
        if (level.useMemory) {
          logDebug(s"Getting block $blockId from memory")
          val result = if (asBlockResult) {
            memoryStore.getValues(blockId).map(new BlockResult(_, DataReadMethod.Memory, info.size))
          } else {
            memoryStore.getBytes(blockId)
          }
          result match {
            case Some(values) =>
              return result
            case None =>
              logDebug(s"Block $blockId not found in memory")
          }
        }

        // Look for the block in Tachyon
        if (level.useOffHeap) {
          logDebug(s"Getting block $blockId from tachyon")
          if (tachyonStore.contains(blockId)) {
            tachyonStore.getBytes(blockId) match {
              case Some(bytes) =>
                if (!asBlockResult) {
                  return Some(bytes)
                } else {
                  return Some(new BlockResult(
                    dataDeserialize(blockId, bytes), DataReadMethod.Memory, info.size))
                }
              case None =>
                logDebug(s"Block $blockId not found in tachyon")
            }
          }
        }
      }
    } else {
      logDebug(s"Block $blockId not registered locally")
    }
    None
  }

  /**
   * Get block from remote block managers.
   *
   * TODO: Remove this method, as the process of retrieving a block from a remote BlockManager
   *       should use a NetworkMonotask instead.
   */
  def getRemote(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting remote block $blockId")
    doGetRemote(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from remote block managers as serialized bytes.
   *
   * TODO: Remove this method, as the process of retrieving a block from a remote BlockManager
   *       should use a NetworkMonotask instead.
   */
  def getRemoteBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting remote block $blockId as bytes")
    doGetRemote(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
  }

  // TODO: Remove this method, as the process of retrieving a block from a remote BlockManager
  //       should use a NetworkMonotask instead.
  private def doGetRemote(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
    require(blockId != null, "BlockId is null")
    val locations = Random.shuffle(master.getLocations(blockId))
    for (loc <- locations) {
      logDebug(s"Getting remote block $blockId from $loc")
      val data = blockTransferService.fetchBlockSync(
        loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()

      if (data != null) {
        if (asBlockResult) {
          return Some(new BlockResult(
            dataDeserialize(blockId, data),
            DataReadMethod.Network,
            data.limit()))
        } else {
          return Some(data)
        }
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /** Get a block from the BlockManager, if it is stored locally. */
  def get(blockId: BlockId): Option[BlockResult] =
    getLocal(blockId)

  def cacheIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(values != null, "Values is null")
    doCache(blockId, IteratorValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializer: Serializer,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): BlockObjectWriter = {
    val compressStream: OutputStream => OutputStream = wrapForCompression(blockId, _)
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(blockId, file, serializer, bufferSize, compressStream, syncWrites,
      writeMetrics)
  }

  /**
   * Cache a new block of values to the block manager. Return a list of blocks updated as a result
   * of this operation.
   */
  def cacheArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(values != null, "Values is null")
    doCache(blockId, ArrayValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Cache a new block of serialized bytes in the block manager. Return a list of blocks updated as
   * a result of this operation.
   */
  def cacheBytes(
      blockId: BlockId,
      bytes: ByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(bytes != null, "Bytes is null")
    doCache(blockId, ByteBufferValues(bytes), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Cache the provided block according to the given level in one of the in-memory block stores
   * (MemoryStore or TachyonStore).
   *
   * The effectiveStorageLevel refers to the level according to which the block will actually be
   * handled. This allows the caller to specify an alternate behavior for doCache while preserving
   * the original level specified by the user.
   *
   * If the block is already stored in memory, tachyon, or on disk, then it will only be cached a
   * second time if it is not already stored at the specified StorageLevel. This can be used to
   * temporarily store blocks in memory that are normally stored on disk so that they can be
   * consumed by ComputeMonotasks. To enable this functionality, if doCache() is called on a block
   * that's already stored at a different StorageLevel, the block will be stored at the new
   * StorageLevel but the BlockInfo's StorageLevel will not be changed.
   */
  private def doCache(
      blockId: BlockId,
      data: BlockValues,
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None)
    : Seq[(BlockId, BlockStatus)] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    effectiveStorageLevel.foreach { effectiveLevel =>
      require(
          (effectiveLevel != null) && effectiveLevel.isValid,
          "Effective StorageLevel is null or invalid")
    }

    // The level we will actually use to cache the block.
    val cachedLevel = effectiveStorageLevel.getOrElse(level)

    // Return value
    val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    // Keep track of whether the block has already been cached so that in the event of a failure, we
    // know not to remove the BlockInfo object if it was already present.
    var alreadyKnown = false
    // Other threads will not be able to get() this block until we call markReady on its BlockInfo.
    val cachedBlockInfo = {
      val tinfo = new BlockInfo(level, tellMaster, None)
      // Do atomically !
      blockInfo.putIfAbsent(blockId, tinfo).map { oldInfo =>
        if (oldInfo.waitForReady()) {
          // We abort the cache operation if the block is already stored where we were going to
          // cache it.
          if ((cachedLevel.useMemory && memoryStore.contains(blockId)) ||
            (cachedLevel.useOffHeap && tachyonStore.contains(blockId))) {
            logWarning(s"Block $blockId already exists on this machine at level $cachedLevel; " +
              "not re-adding it")
            return updatedBlocks
          } else {
            alreadyKnown = true
          }
        }

        // TODO: So the BlockInfo exists, but the previous attempt to load it (?) failed. What do we
        //       do now? Retry on it?
        oldInfo
      }.getOrElse{
        tinfo
      }
    }

    val startTimeMs = System.currentTimeMillis

    // Size of the block in bytes
    var size = 0L

    // TODO: If this block is supposed to be replicated and we are storing bytes, then initiate the
    //       replication here before storing the block locally.

    cachedBlockInfo.synchronized {
      logTrace(s"Caching block $blockId took ${Utils.getUsedTimeMs(startTimeMs)} " +
        "to get into synchronized block.")

      var marked = false
      try {
        // returnValues - Whether to return the cached values
        // blockStore - The type of storage to cache these values in
        val (returnValues, blockStore: InMemoryBlockStore) = {
          if (cachedLevel.useMemory) {
            (true, memoryStore)
          } else if (cachedLevel.useOffHeap) {
            // Use tachyon for off-heap storage.
            (false, tachyonStore)
          } else {
            if (cachedLevel.useDisk) {
              logWarning(s"Attempting to use the BlockManager to write block $blockId to disk. " +
                "This is no longer supported. Use a DiskWriteMonotask instead.")
            }
            throw new BlockException(
              blockId,
              s"Attempted to cache block $blockId with an invalid StorageLevel: $cachedLevel")
          }
        }

        // Actually cache the values
        val result = data match {
          case IteratorValues(iterator) =>
            blockStore.cacheIterator(blockId, iterator, cachedLevel, returnValues)
          case ArrayValues(array) =>
            blockStore.cacheArray(blockId, array, cachedLevel, returnValues)
          case ByteBufferValues(bytes) =>
            bytes.rewind()
            blockStore.cacheBytes(blockId, bytes, cachedLevel)
        }
        size = result.size

        val cachedBlockStatus = getCurrentBlockStatus(blockId, cachedBlockInfo)
        if (cachedBlockStatus.storageLevel != StorageLevel.NONE) {
          // Now that the block has been cached in either the memory or tachyon store, let other
          // threads read it and tell the master about it
          marked = true
          if (!alreadyKnown) {
            // If this is not the first time that this block has been cached, then it has already
            // been marked as ready.
            cachedBlockInfo.markReady(size)
          }
          if (tellMaster) {
            reportBlockStatus(blockId, cachedBlockInfo, cachedBlockStatus)
          }
          updatedBlocks += ((blockId, cachedBlockStatus))
        }
      } finally {
        // If we failed to cache the block, notify other possible readers and then remove it from
        // blockInfo (only remove from blockInfo if this was the first caching attempt for this
        // block).
        if (!marked) {
          // Note that the remove must happen before markFailure otherwise another thread could
          // insert a new BlockInfo object before we remove it.
          if (!alreadyKnown) {
            // The BlockInfo object should only be removed and marked as failed if this was its
            // first caching attempt, since otherwise the block is still available at whatever level
            // it was stored at before this operation began.
            blockInfo.remove(blockId)
            cachedBlockInfo.markFailure()
          }
          logWarning(s"Caching block $blockId failed")
        }
      }
    }
    logDebug(s"Caching block $blockId locally took ${Utils.getUsedTimeMs(startTimeMs)}.")

    // TODO: Reimplement support for block replication using NetworkMonotasks.

    updatedBlocks
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle(blockId: BlockId): Option[Any] = {
    get(blockId).map(_.data.next())
  }

  /**
   * Write a block consisting of a single object.
   */
  def cacheSingle(
      blockId: BlockId,
      value: Any,
      level: StorageLevel,
      tellMaster: Boolean = true): Seq[(BlockId, BlockStatus)] = {
    cacheIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Remove all blocks belonging to the given RDD.
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfo.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logInfo(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfo.keys.collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Removes the specified block from the MemoryStore. The main use case of this method is to remove
   * blocks that were temporarily cached in the MemoryStore so that they could be used by monotasks
   * (for example, the serialized version of a block that is cached in the MemoryStore so that the
   * block can be written to disk by a DiskWriteMonotask).
   */
  def removeBlockFromMemory(blockId: BlockId, tellMaster: Boolean = true) = {
    logInfo(s"Removing block $blockId from the MemoryStore")
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        // Removals are idempotent in memory store. At worst, we get a warning.
        if (memoryStore.remove(blockId)) {
          val status = getCurrentBlockStatus(blockId, info)
          if (status.storageLevel == StorageLevel.NONE) {
            blockInfo.remove(blockId)
            logInfo(s"Block $blockId is no longer stored locally, so the BlockManager discarded " +
              "its metadata.")
          }
          if (tellMaster && info.tellMaster) {
            reportBlockStatus(blockId, info, status)
          }
        } else {
          logWarning(s"Block $blockId could not be removed as it was not found in the MemoryStore.")
        }
      }
    }
  }

  /** Remove a block from memory, tachyon, and disk. */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logInfo(s"Removing block $blockId")
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        // Removals are idempotent in memory store. At worst, we get a warning.
        val removedFromMemory = memoryStore.remove(blockId)
        // TODO: Use a DiskRemoveMonotask to remove the block from disk.
        val removedFromTachyon = if (tachyonInitialized) tachyonStore.remove(blockId) else false
        if (!removedFromMemory && !removedFromTachyon) {
          logWarning(s"Block $blockId could not be removed as it was not found in either " +
            "the memory or tachyon stores")
        }
        blockInfo.remove(blockId)
        if (tellMaster && info.tellMaster) {
          val status = getCurrentBlockStatus(blockId, info)
          reportBlockStatus(blockId, info, status)
        }
      }
    }
  }

  private def dropOldNonBroadcastBlocks(cleanupTime: Long): Unit = {
    logInfo(s"Dropping non broadcast blocks older than $cleanupTime")
    dropOldBlocks(cleanupTime, !_.isBroadcast)
  }

  private def dropOldBroadcastBlocks(cleanupTime: Long): Unit = {
    logInfo(s"Dropping broadcast blocks older than $cleanupTime")
    dropOldBlocks(cleanupTime, _.isBroadcast)
  }

  private def dropOldBlocks(cleanupTime: Long, shouldDrop: (BlockId => Boolean)): Unit = {
    val iterator = blockInfo.getEntrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (id, info, time) = (entry.getKey, entry.getValue.value, entry.getValue.timestamp)
      if (time < cleanupTime && shouldDrop(id)) {
        info.synchronized {
          val level = info.level
          if (level.useMemory) { memoryStore.remove(id) }
          // TODO: Use a DiskRemoveMonotask to remove the block from disk.
          if (level.useOffHeap) { tachyonStore.remove(id) }
          iterator.remove()
          logInfo(s"Dropped block $id")
        }
        val status = getCurrentBlockStatus(id, info)
        reportBlockStatus(id, info, status)
      }
    }
  }

  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _: MonotaskResultBlockId => compressShuffle
      case _ => false
    }
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /** Serializes into a stream. */
  def dataSerializeStream(
      blockId: BlockId,
      outputStream: OutputStream,
      values: Iterator[Any],
      serializer: Serializer = defaultSerializer): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val ser = serializer.newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a byte buffer. */
  def dataSerialize(
      blockId: BlockId,
      values: Iterator[Any],
      serializer: Serializer = defaultSerializer): ByteBuffer = {
    val byteStream = new ByteArrayOutputStream(4096)
    dataSerializeStream(blockId, byteStream, values, serializer)
    ByteBuffer.wrap(byteStream.toByteArray)
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserialize(
      blockId: BlockId,
      bytes: ByteBuffer,
      serializer: Serializer = defaultSerializer): Iterator[Any] = {
    bytes.rewind()
    val stream = wrapForCompression(blockId, new ByteBufferInputStream(bytes, true))
    serializer.newInstance().deserializeStream(stream).asIterator
  }

  /**
   * Updates the specified block's BlockInfo object to reflect that the block is now stored on a
   * particular disk. If the BlockManager does not have a BlockInfo object corresponding to the
   * block, one is created with the provided StorageLevel.
   */
  def updateBlockInfoOnWrite(blockId: BlockId, level: StorageLevel, diskId: String, size: Long) {
    if (blockInfo.contains(blockId)) {
      val info = blockInfo(blockId)
      // Prevent concurrent access to a block's BlockInfo object.
      info.synchronized {
        info.diskId = Some(diskId)
      }
    } else {
      val newInfo = new BlockInfo(level, true, Some(diskId))
      newInfo.markReady(size)
      blockInfo(blockId) = newInfo
    }
    reportBlockStatus(blockId)
  }

  /** Returns a Boolean indicating if the specified block is stored by any BlockManager. */
  def isStored(blockId: BlockId): Boolean =
    blockInfo.contains(blockId) || master.getLocations(blockId).nonEmpty

  /**
   * Returns a Monotask that will load the specified block into the MemoryStore. Returns None if the
   * block is already in the MemoryStore or is not stored by any BlockManager.
   */
  def getBlockLoadMonotask(blockId: BlockId, context: TaskContextImpl): Option[Monotask] = {
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        val level = info.level
        val diskId = info.diskId
        if (level.useMemory && memoryStore.contains(blockId)) {
          return None
        } else if (level.useDisk && blockFileManager.contains(blockId, info.diskId)) {
          return Some(new DiskReadMonotask(context, blockId, diskId.get))
        }
      }
    }
    None
  }

  def stop(): Unit = {
    blockTransferService.close()
    blockFileManager.stop()
    actorSystem.stop(slaveActor)
    blockInfo.clear()
    memoryStore.clear()
    if (tachyonInitialized) {
      tachyonStore.clear()
    }
    metadataCleaner.cancel()
    broadcastCleaner.cancel()
    logInfo("BlockManager stopped")
  }
}

private[spark] object BlockManager extends Logging {
  private val ID_GENERATOR = new IdGenerator

  /** Return the total amount of storage memory available. */
  private def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Unmapping $buffer")
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }

  def blockIdsToBlockManagers(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[BlockManagerId]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[BlockManagerId]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i)
    }
    blockManagers.toMap
  }

  def blockIdsToExecutorIds(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {
    blockIdsToBlockManagers(blockIds, env, blockManagerMaster).mapValues(s => s.map(_.executorId))
  }

  def blockIdsToHosts(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {
    blockIdsToBlockManagers(blockIds, env, blockManagerMaster).mapValues(s => s.map(_.host))
  }
}

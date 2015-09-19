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

import java.io.PrintWriter
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito.{mock, when, spy}

import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkContext, SparkEnv,
  SecurityManager}
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.memory.MemoryShuffleManager
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._


class BlockManagerSuite extends FunSuite with Matchers with BeforeAndAfterEach
  with PrivateMethodTester with ResetSystemProperties {

  private val conf = new SparkConf(false)
  var store: BlockManager = null
  var store2: BlockManager = null
  var actorSystem: ActorSystem = null
  var master: BlockManagerMaster = null
  conf.set("spark.authenticate", "false")
  val securityMgr = new SecurityManager(conf)
  val mapOutputTracker = new MapOutputTrackerMaster(conf)
  val shuffleManager = new MemoryShuffleManager(conf)

  // The BlockFileManager needs to be initialized for each test, because afterEach() stops the
  // BlockManager, which causes the BlockFileManager to delete all of it's local directories.
  var blockFileManager: BlockFileManager = _
  var localDagScheduler: LocalDagScheduler = _

  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  conf.set("spark.kryoserializer.buffer.mb", "1")
  val serializer = new KryoSerializer(conf)

  // Implicitly convert strings to BlockIds for test clarity.
  implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)
  def rdd(rddId: Int, splitId: Int) = RDDBlockId(rddId, splitId)

  private def makeBlockManager(
      maxMem: Long,
      name: String = SparkContext.DRIVER_IDENTIFIER): BlockManager = {
    val transfer = new NettyBlockTransferService(conf, securityMgr, 0)
    val manager = new BlockManager(
      name,
      actorSystem,
      master,
      serializer,
      maxMem,
      maxMem,
      conf,
      mapOutputTracker,
      shuffleManager,
      transfer,
      blockFileManager,
      localDagScheduler)
    manager.initialize("app-id")
    manager
  }

  override def beforeEach(): Unit = {
    blockFileManager = spy(new BlockFileManager(conf))
    localDagScheduler = new LocalDagScheduler(blockFileManager, conf)

    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      "test", "localhost", 0, conf = conf, securityManager = securityMgr)
    this.actorSystem = actorSystem

    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    System.setProperty("os.arch", "amd64")
    conf.set("os.arch", "amd64")
    conf.set("spark.test.useCompressedOops", "true")
    conf.set("spark.driver.port", boundPort.toString)

    master = new BlockManagerMaster(
      actorSystem.actorOf(Props(new BlockManagerMasterActor(true, conf, new LiveListenerBus))),
      conf, true)

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()
  }

  override def afterEach(): Unit = {
    if (store != null) {
      store.stop()
      store = null
    }
    if (store2 != null) {
      store2.stop()
      store2 = null
    }
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    actorSystem = null
    master = null
  }

  test("StorageLevel object caching") {
    val level1 = StorageLevel(false, false, false, false, 3)
    val level2 = StorageLevel(false, false, false, false, 3) // this should return the same object as level1
    val level3 = StorageLevel(false, false, false, false, 2) // this should return a different object
    assert(level2 === level1, "level2 is not same as level1")
    assert(level2.eq(level1), "level2 is not the same object as level1")
    assert(level3 != level1, "level3 is same as level1")
    val bytes1 = Utils.serialize(level1)
    val level1_ = Utils.deserialize[StorageLevel](bytes1)
    val bytes2 = Utils.serialize(level2)
    val level2_ = Utils.deserialize[StorageLevel](bytes2)
    assert(level1_ === level1, "Deserialized level1 not same as original level1")
    assert(level1_.eq(level1), "Deserialized level1 not the same object as original level2")
    assert(level2_ === level2, "Deserialized level2 not same as original level2")
    assert(level2_.eq(level1), "Deserialized level2 not the same object as original level1")
  }

  test("BlockManagerId object caching") {
    val id1 = BlockManagerId("e1", "XXX", 1)
    val id2 = BlockManagerId("e1", "XXX", 1) // this should return the same object as id1
    val id3 = BlockManagerId("e1", "XXX", 2) // this should return a different object
    assert(id2 === id1, "id2 is not same as id1")
    assert(id2.eq(id1), "id2 is not the same object as id1")
    assert(id3 != id1, "id3 is same as id1")
    val bytes1 = Utils.serialize(id1)
    val id1_ = Utils.deserialize[BlockManagerId](bytes1)
    val bytes2 = Utils.serialize(id2)
    val id2_ = Utils.deserialize[BlockManagerId](bytes2)
    assert(id1_ === id1, "Deserialized id1 is not same as original id1")
    assert(id1_.eq(id1), "Deserialized id1 is not the same object as original id1")
    assert(id2_ === id2, "Deserialized id2 is not same as original id2")
    assert(id2_.eq(id1), "Deserialized id2 is not the same object as original id1")
  }

  test("master + 1 manager interaction") {
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

    // Putting a1, a2  and a3 in memory and telling master only about a1 and a2
    store.cacheSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("a3", a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    assert(master.getLocations("a1").size > 0, "master was not told about a1")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
    assert(master.getLocations("a3").size === 0, "master was told about a3")
  }

  // TODO: Reenable this test when the BlockManager supports block replication.
  ignore("master + 2 managers interaction") {
    store = makeBlockManager(2000, "exec1")
    store2 = makeBlockManager(2000, "exec2")

    val peers = master.getPeers(store.blockManagerId)
    assert(peers.size === 1, "master did not return the other manager as a peer")
    assert(peers.head === store2.blockManagerId, "peer returned by master is not the other manager")

    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    store.cacheSingle("a1", a1, StorageLevel.MEMORY_ONLY_2)
    store2.cacheSingle("a2", a2, StorageLevel.MEMORY_ONLY_2)
    assert(master.getLocations("a1").size === 2, "master did not report 2 locations for a1")
    assert(master.getLocations("a2").size === 2, "master did not report 2 locations for a2")
  }

  test("removing block") {
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

    // Putting a1, a2 and a3 in memory and telling master only about a1 and a2
    store.cacheSingle("a1-to-remove", a1, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("a2-to-remove", a2, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("a3-to-remove", a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory and memory size
    val memStatus = master.getMemoryStatus.head._2
    assert(memStatus._1 == 20000L, "total memory " + memStatus._1 + " should equal 20000")
    assert(memStatus._2 <= 12000L, "remaining memory " + memStatus._2 + " should <= 12000")
    assert(store.getSingle("a1-to-remove").isDefined, "a1 was not in store")
    assert(store.getSingle("a2-to-remove").isDefined, "a2 was not in store")
    assert(store.getSingle("a3-to-remove").isDefined, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    assert(master.getLocations("a1-to-remove").size > 0, "master was not told about a1")
    assert(master.getLocations("a2-to-remove").size > 0, "master was not told about a2")
    assert(master.getLocations("a3-to-remove").size === 0, "master was told about a3")

    // Remove a1 and a2 and a3. Should be no-op for a3.
    master.removeBlock("a1-to-remove")
    master.removeBlock("a2-to-remove")
    master.removeBlock("a3-to-remove")

    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a1-to-remove") should be (None)
      master.getLocations("a1-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a2-to-remove") should be (None)
      master.getLocations("a2-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a3-to-remove") should not be (None)
      master.getLocations("a3-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      val memStatus = master.getMemoryStatus.head._2
      memStatus._1 should equal (20000L)
      memStatus._2 should equal (20000L)
    }
  }

  test("removeBlockFromMemory: actually removes the block from the MemoryStore") {
    store = makeBlockManager(1000)
    val blockId = new TestBlockId(0L.toString)
    val block = Array(1, 2, 3, 4).iterator
    store.cacheIterator(blockId, block, StorageLevel.MEMORY_ONLY, false)

    store.removeBlockFromMemory(blockId)
    assert(!store.memoryStore.contains(blockId))
  }

  test("removeBlockFromMemory: deletes the BlockInfo when the block was only in memory") {
    store = makeBlockManager(1000)
    val blockId = new TestBlockId(0L.toString)
    val block = Array(1, 2, 3, 4).iterator
    store.cacheIterator(blockId, block, StorageLevel.MEMORY_ONLY, false)

    store.removeBlockFromMemory(blockId)
    assert(store.getStatus(blockId).isEmpty)
  }

  test("removeBlockFromMemory: doesn't delete the BlockInfo if the block was not just in memory") {
    store = spy(makeBlockManager(1000))
    val blockId = new TestBlockId(0L.toString)
    val diskId = "diskId"
    val diskSizeBytes = 1000L
    val blockFileManager = mock(classOf[BlockFileManager])
    when(blockFileManager.contains(meq(blockId), any())).thenReturn(true)
    when(blockFileManager.getSize(blockId, diskId)).thenReturn(diskSizeBytes)
    when(store.blockFileManager).thenReturn(blockFileManager)
    store.updateBlockInfoOnWrite(blockId, diskId, diskSizeBytes)
    val block = Array(1, 2, 3, 4).iterator
    store.cacheIterator(blockId, block, StorageLevel.MEMORY_ONLY, false)

    store.removeBlockFromMemory(blockId)
    val statusOpt = store.getStatus(blockId)
    assert(statusOpt.isDefined)
    val status = statusOpt.get
    assert(status.storageLevel === StorageLevel.DISK_ONLY)
  }

  test("removing rdd") {
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    // Putting a1, a2 and a3 in memory.
    store.cacheSingle(rdd(0, 0), a1, StorageLevel.MEMORY_ONLY)
    store.cacheSingle(rdd(0, 1), a2, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("nonrddblock", a3, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0, blocking = false)

    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle(rdd(0, 0)) should be (None)
      master.getLocations(rdd(0, 0)) should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle(rdd(0, 1)) should be (None)
      master.getLocations(rdd(0, 1)) should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("nonrddblock") should not be (None)
      master.getLocations("nonrddblock") should have size (1)
    }

    store.cacheSingle(rdd(0, 0), a1, StorageLevel.MEMORY_ONLY)
    store.cacheSingle(rdd(0, 1), a2, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0, blocking = true)
    store.getSingle(rdd(0, 0)) should be (None)
    master.getLocations(rdd(0, 0)) should have size 0
    store.getSingle(rdd(0, 1)) should be (None)
    master.getLocations(rdd(0, 1)) should have size 0
  }

  test("removing broadcast") {
    store = makeBlockManager(5000)
    val driverStore = store
    val executorStore = makeBlockManager(5000, "executor")
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    val a4 = new Array[Byte](400)

    val broadcast0BlockId = BroadcastBlockId(0)
    val broadcast1BlockId = BroadcastBlockId(1)
    val broadcast2BlockId = BroadcastBlockId(2)
    val broadcast2BlockId2 = BroadcastBlockId(2, "_")

    // insert broadcast blocks in both the stores
    Seq(driverStore, executorStore).foreach { case s =>
      s.cacheSingle(broadcast0BlockId, a1, StorageLevel.MEMORY_ONLY)
      s.cacheSingle(broadcast1BlockId, a2, StorageLevel.MEMORY_ONLY)
      s.cacheSingle(broadcast2BlockId, a3, StorageLevel.MEMORY_ONLY)
      s.cacheSingle(broadcast2BlockId2, a4, StorageLevel.MEMORY_ONLY)
    }

    // verify whether the blocks exist in both the stores
    Seq(driverStore, executorStore).foreach { case s =>
      s.getLocal(broadcast0BlockId) should not be (None)
      s.getLocal(broadcast1BlockId) should not be (None)
      s.getLocal(broadcast2BlockId) should not be (None)
      s.getLocal(broadcast2BlockId2) should not be (None)
    }

    // remove broadcast 0 block only from executors
    master.removeBroadcast(0, removeFromMaster = false, blocking = true)

    // only broadcast 0 block should be removed from the executor store
    executorStore.getLocal(broadcast0BlockId) should be (None)
    executorStore.getLocal(broadcast1BlockId) should not be (None)
    executorStore.getLocal(broadcast2BlockId) should not be (None)

    // nothing should be removed from the driver store
    driverStore.getLocal(broadcast0BlockId) should not be (None)
    driverStore.getLocal(broadcast1BlockId) should not be (None)
    driverStore.getLocal(broadcast2BlockId) should not be (None)

    // remove broadcast 0 block from the driver as well
    master.removeBroadcast(0, removeFromMaster = true, blocking = true)
    driverStore.getLocal(broadcast0BlockId) should be (None)
    driverStore.getLocal(broadcast1BlockId) should not be (None)

    // remove broadcast 1 block from both the stores asynchronously
    // and verify all broadcast 1 blocks have been removed
    master.removeBroadcast(1, removeFromMaster = true, blocking = false)
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      driverStore.getLocal(broadcast1BlockId) should be (None)
      executorStore.getLocal(broadcast1BlockId) should be (None)
    }

    // remove broadcast 2 from both the stores asynchronously
    // and verify all broadcast 2 blocks have been removed
    master.removeBroadcast(2, removeFromMaster = true, blocking = false)
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      driverStore.getLocal(broadcast2BlockId) should be (None)
      driverStore.getLocal(broadcast2BlockId2) should be (None)
      executorStore.getLocal(broadcast2BlockId) should be (None)
      executorStore.getLocal(broadcast2BlockId2) should be (None)
    }
    executorStore.stop()
    driverStore.stop()
    store = null
  }

  test("reregistration on heart beat") {
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)

    store.cacheSingle("a1", a1, StorageLevel.MEMORY_ONLY)

    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    implicit val timeout = Timeout(30, TimeUnit.SECONDS)
    val reregister = !Await.result(
      master.driverActor ? BlockManagerHeartbeat(store.blockManagerId),
      timeout.duration).asInstanceOf[Boolean]
    assert(reregister == true)
  }

  test("reregistration on block update") {
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)

    store.cacheSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    store.cacheSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.waitForAsyncReregister()

    assert(master.getLocations("a1").size > 0, "a1 was not reregistered with master")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
  }

  test("reregistration doesn't dead lock") {
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)
    val a2 = List(new Array[Byte](400))

    // try many times to trigger any deadlocks
    for (i <- 1 to 100) {
      master.removeExecutor(store.blockManagerId.executorId)
      val t1 = new Thread {
        override def run() {
          store.cacheIterator("a2", a2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
        }
      }
      val t2 = new Thread {
        override def run() {
          store.cacheSingle("a1", a1, StorageLevel.MEMORY_ONLY)
        }
      }
      val t3 = new Thread {
        override def run() {
          store.reregister()
        }
      }

      t1.start()
      t2.start()
      t3.start()
      t1.join()
      t2.join()
      t3.join()

      store.waitForAsyncReregister()
    }
  }

  test("correct BlockResult returned from get() calls") {
    store = makeBlockManager(12000)
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](500), new Array[Byte](1000), new Array[Byte](1500))
    val list1SizeEstimate = SizeEstimator.estimate(list1.iterator.toArray)
    val list2SizeEstimate = SizeEstimator.estimate(list2.iterator.toArray)
    store.cacheIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.cacheIterator("list2", list2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    val list1Get = store.get("list1")
    assert(list1Get.isDefined, "list1 expected to be in store")
    assert(list1Get.get.data.size === 2)
    assert(list1Get.get.inputMetrics.bytesRead === list1SizeEstimate)
    assert(list1Get.get.inputMetrics.readMethod === DataReadMethod.Memory)
    val list2MemoryGet = store.get("list2")
    assert(list2MemoryGet.isDefined, "list2 expected to be in store")
    assert(list2MemoryGet.get.data.size === 3)
    assert(list2MemoryGet.get.inputMetrics.bytesRead === list2SizeEstimate)
    assert(list2MemoryGet.get.inputMetrics.readMethod === DataReadMethod.Memory)
  }

  test("tachyon storage") {
    // TODO Make the spark.test.tachyon.enable true after using tachyon 0.5.0 testing jar.
    val tachyonUnitTestEnabled = conf.getBoolean("spark.test.tachyon.enable", false)
    if (tachyonUnitTestEnabled) {
      store = makeBlockManager(1200)
      val a1 = new Array[Byte](400)
      val a2 = new Array[Byte](400)
      val a3 = new Array[Byte](400)
      store.cacheSingle("a1", a1, StorageLevel.OFF_HEAP)
      store.cacheSingle("a2", a2, StorageLevel.OFF_HEAP)
      store.cacheSingle("a3", a3, StorageLevel.OFF_HEAP)
      assert(store.getSingle("a3").isDefined, "a3 was in store")
      assert(store.getSingle("a2").isDefined, "a2 was in store")
      assert(store.getSingle("a1").isDefined, "a1 was in store")
    } else {
      info("tachyon storage test disabled.")
    }
  }

  test("negative byte values in ByteBufferInputStream") {
    val buffer = ByteBuffer.wrap(Array[Int](254, 255, 0, 1, 2).map(_.toByte).toArray)
    val stream = new ByteBufferInputStream(buffer)
    val temp = new Array[Byte](10)
    assert(stream.read() === 254, "unexpected byte read")
    assert(stream.read() === 255, "unexpected byte read")
    assert(stream.read() === 0, "unexpected byte read")
    assert(stream.read(temp, 0, temp.length) === 2, "unexpected number of bytes read")
    assert(stream.read() === -1, "end of stream not signalled")
    assert(stream.read(temp, 0, temp.length) === -1, "end of stream not signalled")
  }

  test("block compression") {
    try {
      conf.set("spark.shuffle.compress", "true")
      store = makeBlockManager(20000, "exec1")
      store.cacheSingle(ShuffleBlockId(0, 0, 0), new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(ShuffleBlockId(0, 0, 0)) <= 100,
        "shuffle_0_0_0 was not compressed")
      store.stop()
      store = null

      conf.set("spark.shuffle.compress", "false")
      store = makeBlockManager(20000, "exec2")
      store.cacheSingle(ShuffleBlockId(0, 0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(ShuffleBlockId(0, 0, 0)) >= 10000,
        "shuffle_0_0_0 was compressed")
      store.stop()
      store = null

      conf.set("spark.broadcast.compress", "true")
      store = makeBlockManager(20000, "exec3")
      store.cacheSingle(BroadcastBlockId(0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(BroadcastBlockId(0)) <= 1000,
        "broadcast_0 was not compressed")
      store.stop()
      store = null

      conf.set("spark.broadcast.compress", "false")
      store = makeBlockManager(20000, "exec4")
      store.cacheSingle(BroadcastBlockId(0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(BroadcastBlockId(0)) >= 10000, "broadcast_0 was compressed")
      store.stop()
      store = null

      conf.set("spark.rdd.compress", "true")
      store = makeBlockManager(20000, "exec5")
      store.cacheSingle(rdd(0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(rdd(0, 0)) <= 1000, "rdd_0_0 was not compressed")
      store.stop()
      store = null

      conf.set("spark.rdd.compress", "false")
      store = makeBlockManager(20000, "exec6")
      store.cacheSingle(rdd(0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(rdd(0, 0)) >= 10000, "rdd_0_0 was compressed")
      store.stop()
      store = null

      // Check that any other block types are also kept uncompressed
      store = makeBlockManager(20000, "exec7")
      store.cacheSingle("other_block", new Array[Byte](10000), StorageLevel.MEMORY_ONLY)
      assert(store.memoryStore.getSize("other_block") >= 10000, "other_block was compressed")
      store.stop()
      store = null
    } finally {
      System.clearProperty("spark.shuffle.compress")
      System.clearProperty("spark.broadcast.compress")
      System.clearProperty("spark.rdd.compress")
    }
  }

  test("updated block statuses") {
    store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](2000))
    val bigList = List.fill(8)(new Array[Byte](2000))

    // 1 updated block (i.e. list1)
    val updatedBlocks1 =
      store.cacheIterator("list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks1.size === 1)
    assert(updatedBlocks1.head._1 === TestBlockId("list1"))
    assert(updatedBlocks1.head._2.storageLevel === StorageLevel.MEMORY_ONLY)

    // 1 updated block (i.e. list2)
    val updatedBlocks2 =
      store.cacheIterator("list2", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks2.size === 1)
    assert(updatedBlocks2.head._1 === TestBlockId("list2"))
    assert(updatedBlocks2.head._2.storageLevel === StorageLevel.MEMORY_ONLY)

    // memory store contains all but bigList
    assert(store.memoryStore.contains("list1"), "list1 was not in memory store")
    assert(store.memoryStore.contains("list2"), "list2 was not in memory store")
  }

  test("query block statuses") {
    store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](500))

    store.cacheIterator("list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.cacheIterator("list2", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.cacheIterator("list3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)

    // getLocations and getBlockStatus should yield the same locations
    assert(store.master.getLocations("list1").size === 1)
    assert(store.master.getLocations("list2").size === 1)
    assert(store.master.getLocations("list3").size === 1)
    assert(store.master.getBlockStatus("list1", askSlaves = false).size === 1)
    assert(store.master.getBlockStatus("list2", askSlaves = false).size === 1)
    assert(store.master.getBlockStatus("list3", askSlaves = false).size === 1)
    assert(store.master.getBlockStatus("list1", askSlaves = true).size === 1)
    assert(store.master.getBlockStatus("list2", askSlaves = true).size === 1)
    assert(store.master.getBlockStatus("list3", askSlaves = true).size === 1)

    // This time don't tell master and see what happens.
    store.cacheIterator("list4", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)
    store.cacheIterator("list5", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)
    store.cacheIterator("list6", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // getLocations should return nothing because the master is not informed
    // getBlockStatus without asking slaves should have the same result
    // getBlockStatus with asking slaves, however, should return the actual block statuses
    assert(store.master.getLocations("list4").size === 0)
    assert(store.master.getLocations("list5").size === 0)
    assert(store.master.getLocations("list6").size === 0)
    assert(store.master.getBlockStatus("list4", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list5", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list6", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list4", askSlaves = true).size === 1)
    assert(store.master.getBlockStatus("list5", askSlaves = true).size === 1)
    assert(store.master.getBlockStatus("list6", askSlaves = true).size === 1)
  }

  test("get matching blocks") {
    store = makeBlockManager(12000)
    when(blockFileManager.getAllBlocks()).thenReturn(Seq.empty)
    val list = List.fill(2)(new Array[Byte](100))

    // insert some blocks
    store.cacheIterator("list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.cacheIterator("list2", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.cacheIterator("list3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)

    // getLocations and getBlockStatus should yield the same locations
    assert(store.master.getMatchingBlockIds(_.toString.contains("list"), askSlaves = false).size === 3)
    assert(store.master.getMatchingBlockIds(_.toString.contains("list1"), askSlaves = false).size === 1)

    // insert some more blocks
    store.cacheIterator("newlist1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.cacheIterator("newlist2", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)
    store.cacheIterator("newlist3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // getLocations and getBlockStatus should yield the same locations
    assert(store.master.getMatchingBlockIds(_.toString.contains("newlist"), askSlaves = false).size === 1)
    assert(store.master.getMatchingBlockIds(_.toString.contains("newlist"), askSlaves = true).size === 3)

    val blockIds = Seq(RDDBlockId(1, 0), RDDBlockId(1, 1), RDDBlockId(2, 0))
    blockIds.foreach { blockId =>
      store.cacheIterator(blockId, list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
    val matchedBlockIds = store.master.getMatchingBlockIds(_ match {
      case RDDBlockId(1, _) => true
      case _ => false
    }, askSlaves = true)
    assert(matchedBlockIds.toSet === Set(RDDBlockId(1, 0), RDDBlockId(1, 1)))
  }

  test("unroll blocks using cacheIterator") {
    store = makeBlockManager(12000)
    val memoryStore = store.memoryStore
    val smallList = List.fill(40)(new Array[Byte](100))
    def smallIterator = smallList.iterator.asInstanceOf[Iterator[Any]]

    val result = memoryStore.cacheIterator("b", smallIterator, true, true)
    assert(memoryStore.contains("b"))
    assert(result.size > 0)
    assert(result.data.isLeft)
  }

  test("diskIds are not set when writing to memory") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.cacheSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("a3", a3, StorageLevel.MEMORY_ONLY)
    val disk1 = store.getStatus("a1").get.diskId
    val disk2 = store.getStatus("a2").get.diskId
    val disk3 = store.getStatus("a3").get.diskId
    assert(disk1.isEmpty, "a1's diskId was set")
    assert(disk2.isEmpty, "a2's diskId was set")
    assert(disk3.isEmpty, "a3's diskId was set")
  }

  test("diskIds are not set when writing to off-heap storage") {
    val tachyonUnitTestEnabled = conf.getBoolean("spark.test.tachyon.enable", false)
    if (tachyonUnitTestEnabled) {
      store = makeBlockManager(12000)
      val a1 = new Array[Byte](400)
      val a2 = new Array[Byte](400)
      val a3 = new Array[Byte](400)
      store.cacheSingle("a1", a1, StorageLevel.OFF_HEAP)
      store.cacheSingle("a2", a2, StorageLevel.OFF_HEAP)
      store.cacheSingle("a3", a3, StorageLevel.OFF_HEAP)
      val disk1 = store.getStatus("a1").get.diskId
      val disk2 = store.getStatus("a2").get.diskId
      val disk3 = store.getStatus("a3").get.diskId
      assert(disk1.isEmpty, "a1's diskId was set")
      assert(disk2.isEmpty, "a2's diskId was set")
      assert(disk3.isEmpty, "a3's diskId was set")
    } else {
      info("\"diskIds are not set when writing to off-heap storage\" test disabled.")
    }
  }

  test("the master's diskId records are empty when writing to memory") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.cacheSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.cacheSingle("a3", a3, StorageLevel.MEMORY_ONLY)
    val status1b = master.getBlockStatus("a1", true).get(store.blockManagerId)
    val status2b = master.getBlockStatus("a2", true).get(store.blockManagerId)
    val status3b = master.getBlockStatus("a3", true).get(store.blockManagerId)
    assert(status1b.isDefined, "Block a1 does not have a BlockStatus object")
    assert(status2b.isDefined, "Block a2 does not have a BlockStatus object")
    assert(status3b.isDefined, "Block a3 does not have a BlockStatus object")
    val disk1b = status1b.get.diskId
    val disk2b = status2b.get.diskId
    val disk3b = status3b.get.diskId
    assert(disk1b.isEmpty, "Block a1 has a diskId")
    assert(disk2b.isEmpty, "Block a2 has a diskId")
    assert(disk3b.isEmpty, "Block a3 has a diskId")
  }

  test("the master's diskId records are empty when writing to off-heap storage") {
    val tachyonUnitTestEnabled = conf.getBoolean("spark.test.tachyon.enable", false)
    if (tachyonUnitTestEnabled) {
      store = makeBlockManager(12000)
      val a1 = new Array[Byte](400)
      val a2 = new Array[Byte](400)
      val a3 = new Array[Byte](400)
      store.cacheSingle("a1", a1, StorageLevel.OFF_HEAP)
      store.cacheSingle("a2", a2, StorageLevel.OFF_HEAP)
      store.cacheSingle("a3", a3, StorageLevel.OFF_HEAP)
      val status1b = master.getBlockStatus("a1", true).get(store.blockManagerId)
      val status2b = master.getBlockStatus("a2", true).get(store.blockManagerId)
      val status3b = master.getBlockStatus("a3", true).get(store.blockManagerId)
      assert(status1b.isDefined, "Block a1 does not have a BlockStatus object")
      assert(status2b.isDefined, "Block a2 does not have a BlockStatus object")
      assert(status3b.isDefined, "Block a3 does not have a BlockStatus object")
      val disk1b = status1b.get.diskId
      val disk2b = status2b.get.diskId
      val disk3b = status3b.get.diskId
      assert(disk1b.isEmpty, "Block a1 has a diskId")
      assert(disk2b.isEmpty, "Block a2 has a diskId")
      assert(disk3b.isEmpty, "Block a3 has a diskId")
    } else {
      info("\"diskIds are not sent to the master when writing to off-heap storage\" test disabled.")
    }
  }

  test("updateBlockInfoOnWrite: new storage level recognized for a previously unknown block") {
    store = makeBlockManager(1000)
    val blockId = new TestBlockId("0")
    val diskId = "diskId"
    val diskSizeBytes = 100L
    when(blockFileManager.contains(blockId, Some(diskId))).thenReturn(true)
    when(blockFileManager.getSize(blockId, diskId)).thenReturn(diskSizeBytes)
    store.updateBlockInfoOnWrite(blockId, diskId, diskSizeBytes)

    val statusOpt = store.getStatus(blockId)
    assert(statusOpt.isDefined)
    val status = statusOpt.get
    assert(status.storageLevel === StorageLevel.DISK_ONLY)
    assert(status.diskId.map(_ === diskId).getOrElse(false))
    assert(status.diskSize === diskSizeBytes)
    assert(status.memSize === 0)
    assert(status.tachyonSize === 0)
  }

  test("updateBlockInfoOnWrite: correctly sends a block's status to the master") {
    store = makeBlockManager(1000)
    val blockId = new TestBlockId("0")
    val diskId = "diskId"
    val diskSizeBytes = 100L
    when(blockFileManager.contains(blockId, Some(diskId))).thenReturn(true)
    when(blockFileManager.getSize(blockId, diskId)).thenReturn(diskSizeBytes)
    store.updateBlockInfoOnWrite(blockId, diskId, diskSizeBytes)

    val masterStatusOpt = store.master.getBlockStatus(blockId, true).get(store.blockManagerId)
    assert(masterStatusOpt.isDefined)
    val masterStatus = masterStatusOpt.get
    assert(masterStatus.storageLevel === StorageLevel.DISK_ONLY)
    assert(masterStatus.diskId.map(_ === diskId).getOrElse(false))
    assert(masterStatus.diskSize === diskSizeBytes)
    assert(masterStatus.memSize === 0)
    assert(masterStatus.tachyonSize === 0)
  }

  test("updateBlockInfoOnWrite: all storage levels recognized when a block is already cached") {
    store = makeBlockManager(1000)
    val blockId = new TestBlockId("0")

    // First, store the block in memory.
    val iterator = List(0, 1, 2, 3, 4).iterator
    val updatedBlocks = store.cacheIterator(blockId, iterator, StorageLevel.MEMORY_ONLY, true)

    assert(updatedBlocks.size === 1)
    var statusOpt = store.getStatus(blockId)
    assert(statusOpt.isDefined)
    var status = statusOpt.get
    assert(status.storageLevel === StorageLevel.MEMORY_ONLY)
    assert(status.diskId.isEmpty)
    assert(status.diskSize === 0)
    assert(status.memSize != 0)
    assert(status.tachyonSize === 0)

    // Tell the BlockManager that the block is now on disk and verify that useDisk was added to the
    // storage level.
    val diskId = "diskId"
    val diskSizeBytes = 100L
    when(blockFileManager.contains(blockId, Some(diskId))).thenReturn(true)
    when(blockFileManager.getSize(blockId, diskId)).thenReturn(diskSizeBytes)
    store.updateBlockInfoOnWrite(blockId, diskId, diskSizeBytes)

    statusOpt = store.getStatus(blockId)
    assert(statusOpt.isDefined)
    status = statusOpt.get
    assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)
    assert(status.diskId.map(_ === diskId).getOrElse(false))
    assert(status.diskSize === diskSizeBytes)
    assert(status.memSize != 0)
    assert(status.tachyonSize === 0)
  }

  test("doCache: all storage levels recognized when a block is put at multiple storage levels") {
    store = makeBlockManager(1000)
    val blockId = new TestBlockId("0")

    // Tell the BlockManager that the block is now on disk.
    val diskId = "diskId"
    val diskSizeBytes = 100L
    when(blockFileManager.contains(blockId, Some(diskId))).thenReturn(true)
    when(blockFileManager.getSize(blockId, diskId)).thenReturn(diskSizeBytes)
    store.updateBlockInfoOnWrite(blockId, diskId, diskSizeBytes)

    var statusOpt = store.getStatus(blockId)
    assert(statusOpt.isDefined)
    var status = statusOpt.get
    assert(status.storageLevel === StorageLevel.DISK_ONLY)
    assert(status.diskId.map(_ === diskId).getOrElse(false))
    assert(status.diskSize === diskSizeBytes)
    assert(status.memSize === 0)
    assert(status.tachyonSize === 0)

    // Cache the block in memory too. The new storage level should be MEMORY_AND_DISK_SER.
    val iterator = List(0, 1, 2, 3, 4).iterator
    val updatedBlocks = store.cacheIterator(blockId, iterator, StorageLevel.MEMORY_ONLY_SER, true)

    assert(updatedBlocks.size === 1)
    val (updatedBlockId, _) = updatedBlocks.head
    assert(updatedBlockId === blockId)

    statusOpt = store.getStatus(blockId)
    assert(statusOpt.isDefined)
    status = statusOpt.get
    assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK_SER)
    assert(status.diskId.map(_ === diskId).getOrElse(false))
    assert(status.diskSize === diskSizeBytes)
    assert(status.memSize != 0)
    assert(status.tachyonSize === 0)
  }

  test("doCache: BlockStatus does not change when caching a block twice") {
    store = makeBlockManager(1000)
    val blockId = new TestBlockId("0")

    // Put the block in memory.
    val level = StorageLevel.MEMORY_ONLY
    val iterator = List(0, 1, 2, 3, 4).iterator
    var updatedBlocks = store.cacheIterator(blockId, iterator, level, true)

    assert(updatedBlocks.size === 1)
    assert(updatedBlocks.head._1 === blockId)

    val originalStatusOpt = store.getStatus(blockId)
    assert(originalStatusOpt.isDefined)
    val originalStatus = originalStatusOpt.get
    assert(originalStatus.storageLevel === level)
    assert(originalStatus.diskId.isEmpty)
    assert(originalStatus.diskSize === 0)
    assert(originalStatus.memSize != 0)
    assert(originalStatus.tachyonSize === 0)

    // Try to put the block in memory a second time. This should do nothing.
    updatedBlocks = store.cacheIterator(blockId, iterator, level, true)

    assert(updatedBlocks.isEmpty)
    // Make sure that the BlockStatus has not changed.
    assert(
      store.getStatus(blockId).map(_ === originalStatus).getOrElse(false),
      "BlockInfo modifed or removed by second put.")
  }

  test("doCache: throws an exception when caching a block in both memory and tachyon") {
    val blockId = new TestBlockId("0")
    val iterator = List(0, 1, 2, 3, 4).iterator

    // Verify that caching a block in memory and then tachyon fails.
    store = makeBlockManager(1000)
    store.cacheIterator(blockId, iterator, StorageLevel.MEMORY_ONLY, true)
    intercept[IllegalArgumentException] {
      store.cacheIterator(blockId, iterator, StorageLevel.OFF_HEAP, true)
    }

    if (conf.getBoolean("spark.test.tachyon.enable", false)) {
      // Verify that caching a block in tachyon and then memory fails.
      store = makeBlockManager(1000)
      store.cacheIterator(blockId, iterator, StorageLevel.OFF_HEAP, true)
      intercept[IllegalArgumentException] {
        store.cacheIterator(blockId, iterator, StorageLevel.MEMORY_ONLY, true)
      }
    }
  }

  test("removeBlock removes blocks from disk") {
    store = makeBlockManager(10)

    // Setup a SparkEnv, which is needed so the DiskRemoveMonotask can access the BlockManager and
    // the LocalDagScheduler.
    val env = mock(classOf[SparkEnv])
    when(env.blockManager).thenReturn(store)
    when(env.localDagScheduler).thenReturn(localDagScheduler)
    SparkEnv.set(env)

    // Store a block on disk.
    val blockId = new TestBlockId("testBlock")
    val diskId = blockFileManager.localDirs.keySet.head
    val file = blockFileManager.getBlockFile(blockId, diskId).getOrElse(
      fail("Unable to create file to store test block"))
    val writer = new PrintWriter(file)
    writer.write("This is some test data.")
    writer.close()
    assert(file.exists())
    // Tell the block manager that the block is now stored on-disk.
    val dummySize = 15
    store.updateBlockInfoOnWrite(blockId, diskId, dummySize)

    // Validate that the block is actually stored on disk.
    val status = store.getStatus(blockId).getOrElse(
      fail(s"Block $blockId should be stored in the block manager"))
    assert(status.storageLevel === StorageLevel.DISK_ONLY)

    store.removeBlock(blockId)

    // Make sure the block is eventually deleted from disk.
    eventually(timeout(1000 milliseconds)) { !file.exists() }

    assert(store.getStatus(blockId) === None)
  }
}

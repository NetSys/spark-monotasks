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

import java.io.{File, FileWriter}
import java.util.Random

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

class BlockFileManagerSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private var rootDir1: File = _
  private var rootDir2: File = _
  private var rootDirs: String = _
  private var blockFileManager: BlockFileManager = _
  private var diskIds: Array[String] = _

  override def beforeAll() {
    super.beforeAll()
    rootDir1 = Utils.createTempDir()
    rootDir2 = Utils.createTempDir()
    rootDirs = rootDir1.getAbsolutePath + "," + rootDir2.getAbsolutePath
  }

  override def afterAll() {
    super.afterAll()
    Utils.deleteRecursively(rootDir1)
    Utils.deleteRecursively(rootDir2)
  }

  override def beforeEach() {
    val conf = new SparkConf(false)
    conf.set("spark.local.dir", rootDirs)
    blockFileManager = new BlockFileManager(conf)
    diskIds = blockFileManager.localDirs.keys.toArray
  }

  test("basic block creation") {
    val blockId = new TestBlockId("test")
    assertFileEquals(blockId, blockId.name, 0)
    val fileOpt = blockFileManager.getBlockFile(blockId, diskIds.head)
    assert(fileOpt.isDefined)
    val file = fileOpt.get
    writeToFile(file, 10)
    assertFileEquals(blockId, blockId.name, 10)
    assert(blockFileManager.contains(blockId, Some(diskIds.head)))
    file.delete()
    assert(!blockFileManager.contains(blockId, Some(diskIds.head)))
  }

  test("enumerating blocks") {
    val ids = (1 to 100).map(i => TestBlockId(i.toString())).toSet
    val rand = new Random()
    val numIds = diskIds.length
    ids.foreach { id =>
      val fileOpt = blockFileManager.getBlockFile(id, diskIds(rand.nextInt(numIds)))
      assert(fileOpt.isDefined)
      writeToFile(fileOpt.get, 10)
    }
    assert(blockFileManager.getAllBlocks().toSet === ids)
  }

  def assertFileEquals(blockId: BlockId, filename: String, length: Int) {
    val fileOpt = blockFileManager.getFile(blockId.name, diskIds(0))
    assert(fileOpt.isDefined)
    val file = fileOpt.get
    assert(file.getName === filename)
    assert(file.length() === length)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 1 to numBytes) writer.write(i)
    writer.close()
  }
}

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

package org.apache.spark.scheduler

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.{HashMap, HashSet}
import scala.language.existentials

import org.apache.spark.{Logging, Partition, TaskContextImpl}
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.compute.{ExecutionMonotask, ResultSerializationMonotask}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.ByteBufferInputStream

/**
 * A unit of execution. Spark has two kinds of Macrotasks:
 * - [[org.apache.spark.scheduler.ShuffleMapMacrotask]]
 * - [[org.apache.spark.scheduler.ResultMacrotask]]
 *
 * Macrotasks are used to ship information from the scheduler to the executor, and are responsible
 * for constructing the monotasks needed to complete the Macrotask.
 *
 * A spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultMacrotasks, while earlier stages consist of ShuffleMapMacrotasks. A ResultMacroTask
 * executes the task and sends the task output back to the driver application. A
 * ShuffleMapMacrotask executes the task and divides the task output to multiple buckets (based on
 * the task's partitioner).
 */
private[spark] abstract class Macrotask[T](val stageId: Int, val partition: Partition,
    val dependencyIdToPartitions: HashMap[Long, HashSet[Partition]])
  extends Serializable with Logging {
  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskScheduler.
  var epoch: Long = -1

  /**
   * Deserializes the macrotask binary and returns the RDD that this macrotask will operate on, as
   * well as the ExecutionMonotask that will perform the computation. This function is run within a
   * compute monotask, so should not use network or disk.
   */
  def getExecutionMonotask(context: TaskContextImpl): (RDD[_], ExecutionMonotask[_, _])

  /**
   * Returns the monotasks that need to be run in order to execute this macrotask. This function is
   * run within a compute monotask, so should not use network or disk.
   */
  def getMonotasks(context: TaskContextImpl): Seq[Monotask] = {
    val (rdd, executionMonotask) = getExecutionMonotask(context)
    val resultSerializationMonotask =
      new ResultSerializationMonotask(context, executionMonotask.getResultBlockId())
    resultSerializationMonotask.addDependency(executionMonotask)

    val rddMonotasks =
      rdd.buildDag(partition, dependencyIdToPartitions, context, executionMonotask)
    val leaves = rddMonotasks.filter(_.dependents.isEmpty)
    leaves.foreach(resultSerializationMonotask.addDependency(_))

    rddMonotasks ++ Seq(executionMonotask, resultSerializationMonotask)
  }
}

/**
 * Handles transmission of tasks and their dependencies, because this can be slightly tricky. We
 * need to send the list of JARs and files added to the SparkContext with each task to ensure that
 * worker nodes find out about it, but we can't make it part of the Task because the user's code in
 * the task might depend on one of the JARs. Thus we serialize each task as multiple objects, by
 * first writing out its dependencies.
 */
private[spark] object Macrotask {
  /**
   * Serialize a task and the current app dependencies (files and JARs added to the SparkContext)
   */
  def serializeWithDependencies(
      task: Macrotask[_],
      currentFiles: HashMap[String, Long],
      currentJars: HashMap[String, Long],
      serializer: SerializerInstance)
    : ByteBuffer = {

    val out = new ByteArrayOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    // Write currentFiles
    dataOut.writeInt(currentFiles.size)
    for ((name, timestamp) <- currentFiles) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write currentJars
    dataOut.writeInt(currentJars.size)
    for ((name, timestamp) <- currentJars) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write the task itself and finish
    dataOut.flush()
    val taskBytes = serializer.serialize(task).array()
    out.write(taskBytes)
    ByteBuffer.wrap(out.toByteArray)
  }

  /**
   * Deserialize the list of dependencies in a task serialized with serializeWithDependencies,
   * and return the task itself as a serialized ByteBuffer. The caller can then update its
   * ClassLoaders and deserialize the task.
   *
   * @return (taskFiles, taskJars, taskBytes)
   */
  def deserializeWithDependencies(serializedTask: ByteBuffer)
    : (HashMap[String, Long], HashMap[String, Long], ByteBuffer) = {

    val in = new ByteBufferInputStream(serializedTask)
    val dataIn = new DataInputStream(in)

    // Read task's files
    val taskFiles = new HashMap[String, Long]()
    val numFiles = dataIn.readInt()
    for (i <- 0 until numFiles) {
      taskFiles(dataIn.readUTF()) = dataIn.readLong()
    }

    // Read task's JARs
    val taskJars = new HashMap[String, Long]()
    val numJars = dataIn.readInt()
    for (i <- 0 until numJars) {
      taskJars(dataIn.readUTF()) = dataIn.readLong()
    }

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedTask.slice()  // ByteBufferInputStream will have read just up to task
    (taskFiles, taskJars, subBuffer)
  }
}

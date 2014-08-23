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

package org.apache.spark.rdd

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.spark._
import org.apache.spark.storage.{WarmedShuffleBlockId, ShuffleBlockId}
import org.apache.spark.serializer.{SerializationStream, Serializer}

/**
 * All MiniFetchRDD does is pull together shuffled ByteBuffers that were serialized from a
 * previous ShuffleMap task
 *
 * It's intended only for private use. Use the MiniFetchPipelinedRDD to get functionality
 * similiar to ShuffledRDD
 */
private[spark] class MiniFetchRDD[K, V, C](prev: RDD[_ <: Product2[K, V]], val part: Partitioner)
  extends RDD[ByteBuffer](prev) {

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  override def getDependencies: Seq[Dependency[_]] = {
    List(new MiniFetchDependency(prev, part, None, None, None, false))
    // Note all of the 'None' args; it's because aggregation, etc. is done explicitly in the
    // MiniFetchPipelinedRDD

    // TODO(ryan) MiniFetchDependency needs a refactor. In reality, prev should be of type
    // RDD[ByteBuffer], but there is a lot of code depending on ShuffleDependency that I can't
    // deal with right now, so we make it of type RDD[(Int, ByteBuffer)]. That is, each record
    // is of type (reducePartitionIndex, serializedBytes) but it would be cleaner to just make
    // it serializedBytes and the partition index would just be the index of the bytes
    // within the iterator
  }

  /**
   * Once all parts of given partition have been fetched, it's safe to call
   * compute from a downstream task and it should behave just like a ShuffledRDD
   */
  override def compute(split: Partition, context: TaskContext): Iterator[ByteBuffer] = {
    val ids = shuffleBlockIdsByPartition(split.index).map(WarmedShuffleBlockId.fromShuffle).toArray
    val computed: Array[ByteBuffer] = ids.map { id =>
      val bytes = SparkEnv.get.blockManager.memoryStore.getBytes(id).get
      val isRemoved = SparkEnv.get.blockManager.memoryStore.remove(id)
      assert(isRemoved)
      bytes
    }
    computed.toIterator // TODO(ryan): sort order is not respected
  }

  val shuffleBlockIdsByPartition: Map[Int, Seq[ShuffleBlockId]] = {
    val shuffleId = dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    (0 until this.partitions.length).map {
      reduceId =>
        val mapIds = (0 until prev.partitions.length)
        (reduceId, mapIds.map(ShuffleBlockId(shuffleId, _, reduceId)))
    }.toMap
  }

  override def resource = RDDResource.None
}

/**
 * A MiniFetchRDD which carefully separates the (de)serialization with pipeline tasks
 *
 * The Goal is to seperate out:
 * a) (optionally combine on map side) and serialize before the shuffle write
 * b) shuffle write
 * c) shuffle read
 * d) combine
 */
object MiniFetchPipelinedRDD {

  /** A MiniFetchRDD without an aggregator; it opaquely performs (de-)serialization */
  def apply[K, V](
      prev: RDD[_ <: Product2[K, V]],
      part: Partitioner,
      serializer: Serializer): RDD[(K, V)] = {

    val serialized: RDD[(Int, ByteBuffer)] = prev.mapPartitionsWithContext { // step 1 serialize
        (context, iter) =>
          partitionIterator(iter.asInstanceOf[Iterator[(K, V)]], part, serializer).toIterator
    }

    val pipelined = serialized.pipeline() // step 2 force computation
    val fetched: RDD[ByteBuffer] = new MiniFetchRDD(pipelined, part) // step 3: write and shuffle
    lazy val ser = Serializer.getSerializer(serializer).newInstance()
      // (ser is not serializable, if it's lazy, then the idea is that it'll get created 1 time
      // on remote machine)
    fetched.flatMap(ser.deserializeMany(_).asInstanceOf[Iterator[(K, V)]]) // step 4 deserialize
  }

  // TODO(ryan): this should go somewhere else -> probably in the shuffle package
  /** Partition and serialize an iterator */
  private def partitionIterator(
      iter: Iterator[(_, _)],
      part: Partitioner, serializer: Serializer): Seq[(Int, ByteBuffer)] = {
    val ser = Serializer.getSerializer(serializer).newInstance()
    val streams = Array.tabulate[BufferableByteArrayOutputStream](part.numPartitions) {
      unused => new BufferableByteArrayOutputStream
    }
    val outputs =
      Array.tabulate[SerializationStream](part.numPartitions)(i => ser.serializeStream(streams(i)))

    for ((k, v) <- iter) {
      outputs(part.getPartition(k)).writeObject((k, v))
    }
    streams.zipWithIndex.map{ case(stream, index) => (index, stream.toByteBuffer()) }
  }

  private class BufferableByteArrayOutputStream extends ByteArrayOutputStream {

    /** Create a ByteBuffer backed by the array backing this ByteArrayOutputStream */
    def toByteBuffer() = ByteBuffer.wrap(buf, 0, size())

  }

  /** A MiniFetchRDD with an aggregator */
  def apply[K, V, C](prev: RDD[_ <: Product2[K, V]],
      part: Partitioner,
      serializer: Serializer,
      aggregator: Aggregator[K, V, C],
      mapSideCombine: Boolean): RDD[(K, C)] = {
    if (mapSideCombine) {
      val combined = RDD.aggregate(prev.asInstanceOf[RDD[(K, V)]], aggregator) // aggregate map side
      val fetched = MiniFetchPipelinedRDD(combined, part, serializer)
      RDD.combine(fetched, aggregator) // combine the aggregated records
    } else {
      val fetched = MiniFetchPipelinedRDD(prev, part, serializer)
      RDD.aggregate(fetched, aggregator) // after we've done the shuffle, we explicitly aggregate
    }
  }

}

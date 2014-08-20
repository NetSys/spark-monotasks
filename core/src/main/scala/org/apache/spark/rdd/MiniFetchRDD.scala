package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.storage.{WarmedShuffleBlockId, ShuffleBlockId}
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.serializer.Serializer
import java.nio.ByteBuffer

/**
 * Like ShuffledRDD, but instead requires Fetch mini-tasks to first fetch all parts of
 * it to the local machine; only then can compute() be called on it by a downstream task
 */
class MiniFetchRDD[K, V, C](prev: RDD[_ <: Product2[K, V]], part: Partitioner)
  extends ShuffledRDD[K, V, C](prev, part) {

  override def getDependencies: Seq[Dependency[_]] = {
    List(new MiniFetchDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  /**
   * Once all parts of given partition have been fetched, it's safe to call
   * compute from a downstream task and it should behave just like a ShuffledRDD
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    shuffleBlockIdsByPartition(split.index).flatMap { id =>
      val warmedId = WarmedShuffleBlockId.fromShuffle(id)
      val bytes = SparkEnv.get.blockManager.memoryStore.getBytes(warmedId).get
      SparkEnv.get.blockManager.dataDeserialize(warmedId, bytes, Serializer.getSerializer(dep.serializer))
    }.toIterator.asInstanceOf[Iterator[(K, C)]] // TODO(ryan): sort order is not respected
  }

  // TODO(ryan): if isn't lazy, the this.dependencies will be called too early and
  // not capture added deps
  lazy val shuffleBlockIdsByPartition: Map[Int, Seq[ShuffleBlockId]] = {
    val shuffleId = dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    (0 until this.partitions.length).map {
      reduceId =>
        val mapIds = (0 until prev.partitions.length)
        (reduceId, mapIds.map(ShuffleBlockId(shuffleId, _, reduceId)))
    }.toMap
  }

  override def resource = RDDResourceTypes.None

  override def free(partition: Partition) {
    for (id <- shuffleBlockIdsByPartition(partition.index)) {
      SparkEnv.get.blockManager.memoryStore.remove(id)
    }
  }
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

  /** A MiniFetchRDD without an aggregator */
  def apply[K, V](prev: RDD[_ <: Product2[K, V]], part: Partitioner, serializer: Serializer): RDD[(K, V)] = {


    val serialized: RDD[(K, Array[Byte])] = prev.map[(K, Array[Byte])] { kv =>
        val ser = Serializer.getSerializer(serializer).newInstance() //TODO(ryan) don't make a new one for each record!
        (kv._1, ser.serialize((kv._1, kv._2)).array()) // TODO(ryan): can key just be partition # (maybe not if want sorted)
    }
    val fetched: RDD[(K, Array[Byte])] = new MiniFetchRDD(serialized, part) // note the lack of aggregator, we can add
                                                                           // it with the other utility functions
    fetched.map {kv =>
      val ser = Serializer.getSerializer(serializer).newInstance()
      ser.deserialize[(K, V)](ByteBuffer.wrap(kv._2))
    }
  }

  /** A MiniFetchRDD with an aggregator */
  def apply[K, V, C](prev: RDD[_ <: Product2[K, V]],
      part: Partitioner,
      serializer: Serializer,
      aggregator: Aggregator[K, V, C],
      mapSideCombine: Boolean): RDD[(K, C)] = {
    if (mapSideCombine) {
      aggregateWithCombine[K, V, C](prev, part, serializer, aggregator)
    } else {
      aggregateWithoutCombine(prev, part, serializer, aggregator)
    }
  }

  private def aggregateWithoutCombine[K, V, C](prev: RDD[_ <: Product2[K, V]],
       part: Partitioner,
       serializer: Serializer,
       aggregator: Aggregator[K, V, C]): RDD[(K, C)] = {
    val fetched = MiniFetchPipelinedRDD(prev, part, serializer)
    RDD.aggregate(fetched, aggregator)
  }

  private def aggregateWithCombine[K, V, C](prev: RDD[_ <: Product2[K, V]],
      part: Partitioner,
      serializer: Serializer,
      aggregator: Aggregator[K, V, C]): RDD[(K, C)] = {
     val combined = RDD.aggregate(prev.asInstanceOf[RDD[(K, V)]], aggregator)
     val fetched = MiniFetchPipelinedRDD(combined, part, serializer)
     RDD.combine(fetched, aggregator)
  }
}

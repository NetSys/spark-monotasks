package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.storage.{ShuffleBlockId}

/**
 * Like ShuffledRDD, but instead requires Fetch mini-tasks to first fetch all parts of
 * it to the local machine; only then can compute() be called on it by a downstream task
 */
class MiniFetchRDD[K, V, C](prev: RDD[_ <: Product2[K, V]], part: Partitioner)
  extends ShuffledRDD[K, V, C](prev, part) {

  /**
   * Once all parts of given partition have been fetched, it's safe to call
   * compute from a downstream task and it should behave just like a ShuffledRDD
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    shuffleBlockIdsByPartition(split.index).flatMap {
      SparkEnv.get.blockManager.memoryStore.getValues
    }.asInstanceOf[Iterator[(K, C)]]
  }

  val shuffleBlockIdsByPartition: Map[Int, Seq[ShuffleBlockId]] = {
    val shuffleId = dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    (0 until this.partitions.length).map {
      reduceId =>
        val mapIds = (0 until prev.partitions.length)
        (reduceId, mapIds.map(ShuffleBlockId(shuffleId, _, reduceId)))
    }.toMap
  }

}

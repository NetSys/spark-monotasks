package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.storage.{ShuffleBlockId}
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.serializer.Serializer

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
    val iter = shuffleBlockIdsByPartition(split.index).flatMap {
      SparkEnv.get.blockManager.memoryStore.getValues(_).get
    }.toIterator.asInstanceOf[Iterator[Product2[K, C]]]
    aggregateAndStuff(iter, context).asInstanceOf[Iterator[(K, C)]]
  }

  // TODO(ryan): this is copied from HashShuffleReader (it doesn't seem easy to refactor
  // that code to avoid the copy paste)
  private def aggregateAndStuff(iter: Iterator[Product2[K, C]],
                                context: TaskContext): Iterator[Product2[K, C]] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    val ser = Serializer.getSerializer(dep.serializer)
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        new InterruptibleIterator(context, dep.aggregator.get.combineCombinersByKey(iter, context))
      } else {
        new InterruptibleIterator(context,
          dep.aggregator.get.combineValuesByKey(iter.asInstanceOf[Iterator[Product2[K, V]]], context))
      }
    } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      iter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter = new ExternalSorter[K, C, C](ordering = Some(keyOrd), serializer = Some(ser))
        sorter.write(aggregatedIter)
        context.taskMetrics.memoryBytesSpilled += sorter.memoryBytesSpilled
        context.taskMetrics.diskBytesSpilled += sorter.diskBytesSpilled
        sorter.iterator
      case None =>
        aggregatedIter
    }
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
}

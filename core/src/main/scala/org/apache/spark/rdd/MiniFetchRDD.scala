package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.storage.{WarmedShuffleBlockId, ShuffleBlockId}
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, Serializer}
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream

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
    List(new MiniFetchDependency(prev, part, None, None, None, false)) // TODO(ryan) MiniFetchDependency needs a refactor
  }

  /**
   * Once all parts of given partition have been fetched, it's safe to call
   * compute from a downstream task and it should behave just like a ShuffledRDD
   */
  override def compute(split: Partition, context: TaskContext): Iterator[ByteBuffer] = {
    shuffleBlockIdsByPartition(split.index).iterator.map { id =>
      val warmedId = WarmedShuffleBlockId.fromShuffle(id)
      SparkEnv.get.blockManager.memoryStore.getBytes(warmedId).get
    } // TODO(ryan): sort order is not respected
  }

	val shuffleBlockIdsByPartition: Map[Int, Seq[ShuffleBlockId]] = {
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

  /** A MiniFetchRDD without an aggregator; it opaquely performs the serialization and deserialization */
  def apply[K, V](prev: RDD[_ <: Product2[K, V]], part: Partitioner, serializer: Serializer): RDD[(K, V)] = {

    val serialized: RDD[(Int, ByteBuffer)] = prev.mapPartitionsWithContext { // step 1 serialize
        (context, iter) => partitionIterator(iter.asInstanceOf[Iterator[(K, V)]], part, serializer)
    }

    val pipelined = serialized.pipeline() // step 2 force computation
    val fetched: RDD[ByteBuffer] = new MiniFetchRDD(pipelined, part) // step 3 will cause a write and then a shuffle
    lazy val ser = Serializer.getSerializer(serializer).newInstance() // (ser is not serializable, if it's lazy, then
                                                                      // the idea is that it'll get created on remote machine)
    fetched.flatMap(ser.deserializeMany(_).asInstanceOf[Iterator[(K, V)]]) // step 4 deserialize
  }

  // TODO(ryan): this should go somewhere else -> probably in the shuffle package
  private def partitionIterator(iter: Iterator[(_, _)], part: Partitioner, serializer: Serializer): Iterator[(Int, ByteBuffer)] = {
    val ser = Serializer.getSerializer(serializer).newInstance()
    val streams = Array.tabulate[BufferableByteArrayOutputStream](part.numPartitions) {
      unused => new BufferableByteArrayOutputStream
    }
    val outputs = Array.tabulate[SerializationStream](part.numPartitions)(i => ser.serializeStream(streams(i)))

    for ((k, v) <- iter) {
      outputs(part.getPartition(k)).writeObject((k, v))
    }
    streams.zipWithIndex.map{ case(stream, index) => (index, stream.toByteBuffer()) }.toIterator
  }

  class BufferableByteArrayOutputStream extends ByteArrayOutputStream {

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

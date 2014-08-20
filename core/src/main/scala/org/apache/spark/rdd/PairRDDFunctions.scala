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

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, HashMap => JHashMap}

import scala.collection.{Map, mutable}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat,
RecordWriter => NewRecordWriter, SparkHadoopMapReduceUtil}

import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.Experimental
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.random.StratifiedSamplingUtils

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 * Import `org.apache.spark.SparkContext._` at the top of your program to use these functions.
 */
class PairRDDFunctions[K, V](self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging
  with SparkHadoopMapReduceUtil
  with Serializable
{
  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   */
  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null): RDD[(K, C)] = {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("Default partitioner cannot partition array keys.")
      }
    }
    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitionsWithContext((context, iter) => {
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
      MiniFetchPipelinedRDD[K, V, C](self, partitioner, serializer, aggregator, mapSideCombine)
    }
  }

  /**
   * Simplified version of combineByKey that hash-partitions the output RDD.
   */
  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numPartitions: Int): RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numPartitions))
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.closureSerializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    lazy val cachedSerializer = SparkEnv.get.closureSerializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    combineByKey[U]((v: V) => seqOp(createZero(), v), seqOp, combOp, partitioner)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = {
    aggregateByKey(zeroValue, new HashPartitioner(numPartitions))(seqOp, combOp)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = {
    aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] = {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.closureSerializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // When deserializing, use a lazy val to create just one instance of the serializer per task
    lazy val cachedSerializer = SparkEnv.get.closureSerializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[V](ByteBuffer.wrap(zeroArray))

    combineByKey[V]((v: V) => func(createZero(), v), func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] = {
    foldByKey(zeroValue, new HashPartitioner(numPartitions))(func)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = {
    foldByKey(zeroValue, defaultPartitioner(self))(func)
  }

  /**
   * Return a subset of this RDD sampled by key (via stratified sampling).
   *
   * Create a sample of this RDD using variable sampling rates for different keys as specified by
   * `fractions`, a key to sampling rate map.
   *
   * If `exact` is set to false, create the sample via simple random sampling, with one pass
   * over the RDD, to produce a sample of size that's approximately equal to the sum of
   * math.ceil(numItems * samplingRate) over all key values; otherwise, use
   * additional passes over the RDD to create a sample size that's exactly equal to the sum of
   * math.ceil(numItems * samplingRate) over all key values with a 99.99% confidence. When sampling
   * without replacement, we need one additional pass over the RDD to guarantee sample size;
   * when sampling with replacement, we need two additional passes.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @param seed seed for the random number generator
   * @param exact whether sample size needs to be exactly math.ceil(fraction * size) per key
   * @return RDD containing the sampled subset
   */
  def sampleByKey(withReplacement: Boolean,
      fractions: Map[K, Double],
      exact: Boolean = false,
      seed: Long = Utils.random.nextLong): RDD[(K, V)]= {

    require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

    val samplingFunc = if (withReplacement) {
      StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, exact, seed)
    } else {
      StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, exact, seed)
    }
    self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative reduce function, but return the results
   * immediately to the master as a Map. This will also perform the merging locally on each mapper
   * before sending results to a reducer, similarly to a "combiner" in MapReduce.
   */
  def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = {

    if (keyClass.isArray) {
      throw new SparkException("reduceByKeyLocally() does not support array keys")
    }

    val reducePartition = (iter: Iterator[(K, V)]) => {
      val map = new JHashMap[K, V]
      iter.foreach { pair =>
        val old = map.get(pair._1)
        map.put(pair._1, if (old == null) pair._2 else func(old, pair._2))
      }
      Iterator(map)
    } : Iterator[JHashMap[K, V]]

    val mergeMaps = (m1: JHashMap[K, V], m2: JHashMap[K, V]) => {
      m2.foreach { pair =>
        val old = m1.get(pair._1)
        m1.put(pair._1, if (old == null) pair._2 else func(old, pair._2))
      }
      m1
    } : JHashMap[K, V]

    self.mapPartitions(reducePartition).reduce(mergeMaps)
  }

  /** Alias for reduceByKeyLocally */
  @deprecated("Use reduceByKeyLocally", "1.0.0")
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = reduceByKeyLocally(func)

  /** Count the number of elements for each key, and return the result to the master as a Map. */
  def countByKey(): Map[K, Long] = self.map(_._1).countByValue()

  /**
   * :: Experimental ::
   * Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
   */
  @Experimental
  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
      : PartialResult[Map[K, BoundedDouble]] = {
    self.map(_._1).countByValueApprox(timeout, confidence)
  }

  /**
   * :: Experimental ::
   *
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero `sp > p`
   * would trigger sparse representation of registers, which may reduce the memory consumption
   * and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   * @param partitioner Partitioner to use for the resulting RDD.
   */
  @Experimental
  def countApproxDistinctByKey(p: Int, sp: Int, partitioner: Partitioner): RDD[(K, Long)] = {
    require(p >= 4, s"p ($p) must be >= 4")
    require(sp <= 32, s"sp ($sp) must be <= 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val createHLL = (v: V) => {
      val hll = new HyperLogLogPlus(p, sp)
      hll.offer(v)
      hll
    }
    val mergeValueHLL = (hll: HyperLogLogPlus, v: V) => {
      hll.offer(v)
      hll
    }
    val mergeHLL = (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
      h1.addAll(h2)
      h1
    }

    combineByKey(createHLL, mergeValueHLL, mergeHLL, partitioner).mapValues(_.cardinality())
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param partitioner partitioner of the resulting RDD
   */
  def countApproxDistinctByKey(relativeSD: Double, partitioner: Partitioner): RDD[(K, Long)] = {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    assert(p <= 32)
    countApproxDistinctByKey(if (p < 4) 4 else p, 0, partitioner)
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param numPartitions number of partitions of the resulting RDD
   */
  def countApproxDistinctByKey(relativeSD: Double, numPartitions: Int): RDD[(K, Long)] = {
    countApproxDistinctByKey(relativeSD, new HashPartitioner(numPartitions))
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinctByKey(relativeSD: Double = 0.05): RDD[(K, Long)] = {
    countApproxDistinctByKey(relativeSD, defaultPartitioner(self))
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs = combineByKey[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine=false)
    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numPartitions` partitions.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = {
    groupByKey(new HashPartitioner(numPartitions))
  }

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   */
  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = {
    if (keyClass.isArray && partitioner.isInstanceOf[HashPartitioner]) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    if (self.partitioner == Some(partitioner)) {
      self
    } else {
      new ShuffledRDD[K, V, V](self, partitioner)
    }
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   */
  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = {
    this.cogroup(other, partitioner).flatMapValues( pair =>
      for (v <- pair._1; w <- pair._2) yield (v, w)
    )
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def leftOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))] = {
    this.cogroup(other, partitioner).flatMapValues { pair =>
      if (pair._2.isEmpty) {
        pair._1.map(v => (v, None))
      } else {
        for (v <- pair._1; w <- pair._2) yield (v, Some(w))
      }
    }
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], W))] = {
    this.cogroup(other, partitioner).flatMapValues { pair =>
      if (pair._1.isEmpty) {
        pair._2.map(w => (None, w))
      } else {
        for (v <- pair._1; w <- pair._2) yield (Some(v), w)
      }
    }
  }

  /**
   * Simplified version of combineByKey that hash-partitions the resulting RDD using the
   * existing partitioner/parallelism level.
   */
  def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)
    : RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self))
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    reduceByKey(defaultPartitioner(self), func)
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the existing partitioner/parallelism level.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupByKey(): RDD[(K, Iterable[V])] = {
    groupByKey(defaultPartitioner(self))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    join(other, defaultPartitioner(self, other))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = {
    join(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
   */
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
   */
  def leftOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
   */
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def rightOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return the key-value pairs in this RDD to the master as a Map.
   *
   * Warning: this doesn't return a multimap (so if you have multiple values to the same key, only
   *          one value per key is preserved in the map returned)
   */
  def collectAsMap(): Map[K, V] = {
    val data = self.collect()
    val map = new mutable.HashMap[K, V]
    map.sizeHint(data.length)
    data.foreach { pair => map.put(pair._1, pair._2) }
    map
  }

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  def mapValues[U](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD(self, cleanF)
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   */
  def flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new FlatMappedValuesRDD(self, cleanF)
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      other3: RDD[(K, W3)],
      partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2, other3), partitioner)
    cg.mapValues { case Array(vs, w1s, w2s, w3s) =>
       (vs.asInstanceOf[Iterable[V]],
         w1s.asInstanceOf[Iterable[W1]],
         w2s.asInstanceOf[Iterable[W2]],
         w3s.asInstanceOf[Iterable[W3]])
    }
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W]))]  = {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other), partitioner)
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2), partitioner)
    cg.mapValues { case Array(vs, w1s, w2s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]],
        w2s.asInstanceOf[Iterable[W2]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = {
    cogroup(other, defaultPartitioner(self, other))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))] = {
    cogroup(other, new HashPartitioner(numPartitions))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = {
    cogroup(other1, other2, new HashPartitioner(numPartitions))
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      other3: RDD[(K, W3)],
      numPartitions: Int)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = {
    cogroup(other1, other2, other3, new HashPartitioner(numPartitions))
  }

  /** Alias for cogroup. */
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = {
    cogroup(other, defaultPartitioner(self, other))
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   */
  def subtractByKey[W: ClassTag](other: RDD[(K, W)]): RDD[(K, V)] =
    subtractByKey(other, self.partitioner.getOrElse(new HashPartitioner(self.partitions.size)))

  /** Return an RDD with the pairs from `this` whose keys are not in `other`. */
  def subtractByKey[W: ClassTag](other: RDD[(K, W)], numPartitions: Int): RDD[(K, V)] =
    subtractByKey(other, new HashPartitioner(numPartitions))

  /** Return an RDD with the pairs from `this` whose keys are not in `other`. */
  def subtractByKey[W: ClassTag](other: RDD[(K, W)], p: Partitioner): RDD[(K, V)] =
    new SubtractedRDD[K, V, W](self, other, p)

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   */
  def lookup(key: K): Seq[V] = {
    self.partitioner match {
      case Some(p) =>
        val index = p.getPartition(key)
        val process = (it: Iterator[(K, V)]) => {
          val buf = new ArrayBuffer[V]
          for (pair <- it if pair._1 == key) {
            buf += pair._2
          }
          buf
        } : Seq[V]
        val res = self.context.runJob(self, process, Array(index), false)
        res(0)
      case None =>
        self.filter(_._1 == key).map(_._2).collect()
    }
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](path: String)(implicit fm: ClassTag[F]) {
    saveAsHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress the result with the
   * supplied codec.
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](
      path: String, codec: Class[_ <: CompressionCodec]) (implicit fm: ClassTag[F]) {
    val runtimeClass = fm.runtimeClass
    saveAsHadoopFile(path, keyClass, valueClass, runtimeClass.asInstanceOf[Class[F]], codec)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](path: String)(implicit fm: ClassTag[F]) {
    saveAsNewAPIHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   */
  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = self.context.hadoopConfiguration)
  {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val job = new NewAPIHadoopJob(hadoopConf)
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    job.setOutputFormatClass(outputFormatClass)
    job.getConfiguration.set("mapred.output.dir", path)
    saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress with the supplied codec.
   */
  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      codec: Class[_ <: CompressionCodec]) {
    saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass,
      new JobConf(self.context.hadoopConfiguration), Some(codec))
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   */
  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf(self.context.hadoopConfiguration),
      codec: Option[Class[_ <: CompressionCodec]] = None) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    hadoopConf.setOutputKeyClass(keyClass)
    hadoopConf.setOutputValueClass(valueClass)
    // Doesn't work in Scala 2.9 due to what may be a generics bug
    // TODO: Should we uncomment this for Scala 2.10?
    // conf.setOutputFormat(outputFormatClass)
    hadoopConf.set("mapred.output.format.class", outputFormatClass.getName)
    for (c <- codec) {
      hadoopConf.setCompressMapOutput(true)
      hadoopConf.set("mapred.output.compress", "true")
      hadoopConf.setMapOutputCompressorClass(c)
      hadoopConf.set("mapred.output.compression.codec", c.getCanonicalName)
      hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    }
    hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    FileOutputFormat.setOutputPath(hadoopConf,
      SparkHadoopWriter.createPathFromString(path, hadoopConf))
    saveAsHadoopDataset(hadoopConf)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system with new Hadoop API, using a Hadoop
   * Configuration object for that storage system. The Conf should set an OutputFormat and any
   * output paths required (e.g. a table name to write to) in the same way as it would be
   * configured for a Hadoop MapReduce job.
   */
  def saveAsNewAPIHadoopDataset(conf: Configuration) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val job = new NewAPIHadoopJob(hadoopConf)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    val outfmt = job.getOutputFormatClass
    val jobFormat = outfmt.newInstance

    if (self.conf.getBoolean("spark.hadoop.validateOutputSpecs", true)) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    val writeShard = (context: TaskContext, iter: Iterator[(K,V)]) => {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        attemptNumber)
      val hadoopContext = newTaskAttemptContext(wrappedConf.value, attemptId)
      val format = outfmt.newInstance
      format match {
        case c: Configurable => c.setConf(wrappedConf.value)
        case _ => ()
      }
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)
      val writer = format.getRecordWriter(hadoopContext).asInstanceOf[NewRecordWriter[K,V]]
      try {
        while (iter.hasNext) {
          val pair = iter.next()
          writer.write(pair._1, pair._2)
        }
      } finally {
        writer.close(hadoopContext)
      }
      committer.commitTask(hadoopContext)
      1
    } : Int

    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    self.context.runJob(self, writeShard)
    jobCommitter.commitJob(jobTaskContext)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
   */
  def saveAsHadoopDataset(conf: JobConf) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val outputFormatInstance = hadoopConf.getOutputFormat
    val keyClass = hadoopConf.getOutputKeyClass
    val valueClass = hadoopConf.getOutputValueClass
    if (outputFormatInstance == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    SparkHadoopUtil.get.addCredentials(hadoopConf)

    logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
      valueClass.getSimpleName + ")")

    if (self.conf.getBoolean("spark.hadoop.validateOutputSpecs", true)) {
      // FileOutputFormat ignores the filesystem parameter
      val ignoredFs = FileSystem.get(hadoopConf)
      hadoopConf.getOutputFormat.checkOutputSpecs(ignoredFs, hadoopConf)
    }

    val writer = new SparkHadoopWriter(hadoopConf)
    writer.preSetup()

    val writeToFile = (context: TaskContext, iter: Iterator[(K, V)]) => {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt

      writer.setup(context.stageId, context.partitionId, attemptNumber)
      writer.open()
      try {
        var count = 0
        while (iter.hasNext) {
          val record = iter.next()
          count += 1
          writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])
        }
      } finally {
        writer.close()
      }
      writer.commit()
    }

    self.context.runJob(self, writeToFile)
    writer.commitJob()
  }

  /**
   * Return an RDD with the keys of each tuple.
   */
  def keys: RDD[K] = self.map(_._1)

  /**
   * Return an RDD with the values of each tuple.
   */
  def values: RDD[V] = self.map(_._2)

  private[spark] def keyClass: Class[_] = kt.runtimeClass

  private[spark] def valueClass: Class[_] = vt.runtimeClass

  private[spark] def keyOrdering: Option[Ordering[K]] = Option(ord)
}

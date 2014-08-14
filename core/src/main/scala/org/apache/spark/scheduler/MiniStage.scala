package org.apache.spark.scheduler

import org.apache.spark.rdd.{MiniFetchRDD, RDD}
import org.apache.spark._
import scala.collection.mutable
import scala.collection.mutable.HashMap
import org.apache.spark.storage.{ShuffleBlockId, BlockId}

/**
 * A MiniStage allows tracking of dependencies between tasks in the same Stage.
 * That is, multiple MiniStages run in a given stage and there are dependencies
 * from tasks in one MiniStage to those of another.
 *
 * MiniStages should be constructed in two ways, as defined in the MiniStage object.
 */
abstract class MiniStage(val stageId: Int, val dependencies: Seq[MiniStage]) {

  /** all tasks in this MiniStage */
  def tasks: Seq[Task[_]]

  /** For a task of the next stage, find the tasks of this stage that are dependencies for it */
  def dependenciesOfChild(task: Task[_]): Seq[Task[_]]

}

/**
 * A MiniStage whose dependent Tasks have OneToOneDependencies to this MiniStage
 */
abstract class OneToOneStage(stageId: Int, val rdd: RDD[_], dependencies: Seq[MiniStage], val scheduler: DAGScheduler)
  extends MiniStage(stageId, dependencies) {

  lazy val taskByPartitionMap: Map[Partition, Task[_]] = rdd.partitions.map(part => (part, taskByPartition(part))).toMap
  // TODO(ryan): if it isn't lazy, then there is some problem with bin serialization (prob happens too early?)

  def taskByPartition(partition: Partition): Task[_]

  override def tasks: Seq[Task[_]] = rdd.partitions.map {taskByPartitionMap(_)}

  def dependenciesOfChild(task: Task[_]): Seq[Task[_]] = Seq(taskByPartitionMap(rdd.partitions(task.partitionId)))
  // TODO(ryan) assuming that partitionId is an index into rdd.partitions -- is it?
  // TODO(ryan) assuming that deps are all 1-1 for now...
  // TODO(ryan): Tasks all have the same StageId regardless of what MiniStage they are in, which could lead to conflicts
  // where tasks look the same (they have same StageId, Partition), but they in fact are on different RDDs
  // I'm using the taskByPartitionMap do equality based on references which is a brittle stop-gap

}

/**
 * A type of MiniStage holding PipelineTasks
 */
class PipelineStage(stageId: Int, rdd: RDD[_], dependencies: Seq[MiniStage], scheduler: DAGScheduler)
  extends OneToOneStage(stageId, rdd, dependencies, scheduler) {

  val binary = scheduler.getBinary(rdd)

  override def taskByPartition(partition: Partition): PipelineTask = {
    new PipelineTask(stageId, binary, partition, scheduler.getPreferredLocs(rdd, partition.index))
  }

}

/**
 * A type of MiniStage holding MiniFetchTasks
 */
class MiniFetchStage(stageId: Int, outputRDD: MiniFetchRDD[_, _, _], dep: MiniFetchDependency[_, _, _], scheduler: DAGScheduler)
  extends MiniStage(stageId, Seq()) {

  val statuses = SparkEnv.get.mapOutputTracker.getMapStatuses(dep.shuffleId)

  // TODO(ryan): weird bug below if using mapValues where mapValues would call the function multiple times ...
  // it seems like a bug in the implementation of Map.mapValues
  private val tasksByPartition: Map[Int, Seq[Task[_]]] =
    outputRDD.shuffleBlockIdsByPartition.toList.map { case (k, v) => (k, v.map(taskFromShuffleBlock _)) }.toMap

  private def taskFromShuffleBlock(id: ShuffleBlockId): Task[_] = {
    val status = statuses(id.mapId)
    new MiniFetchTask(stageId, scheduler.getBinary(id, status.location, status.compressedSizes(id.reduceId), dep))
  }

  override def tasks: Seq[Task[_]] = tasksByPartition.values.flatten.toSeq

  override def dependenciesOfChild(child: Task[_]): Seq[Task[_]] = tasksByPartition(child.partitionId)

}

/**
 * A type of MiniStage holding ShuffleMapTasks (this MiniStage is always the last to be run in
 * a given Stage)
 */
class ShuffleMapStage(stageId: Int, rdd: RDD[_], dependencies: Seq[MiniStage], shuffleDep: ShuffleDependency[_, _, _],
                      scheduler: DAGScheduler)
  extends OneToOneStage(stageId, rdd, dependencies, scheduler) {

  val binary = scheduler.getBinary((rdd, shuffleDep))

  override def taskByPartition(partition: Partition): ShuffleMapTask = {
    new ShuffleMapTask(stageId, binary, partition, scheduler.getPreferredLocs(rdd, partition.index))
  }

}

/**
 * A type of MiniStage holding ShuffleMapTasks (this MiniStage is always the last to be run in
 * a given Stage)
 */
class ResultStage(stageId: Int, rdd: RDD[_], dependencies: Seq[MiniStage],
                  func: (TaskContext, scala.Iterator[_]) => (_$1) forSome {type _$1},
                   job: ActiveJob, scheduler: DAGScheduler)
  extends OneToOneStage(stageId, rdd, dependencies, scheduler) {

  val binary = scheduler.getBinary((rdd, func))

  override def taskByPartition(partition: Partition): Task[_] = {
    new ResultTask(stageId, binary, partition, scheduler.getPreferredLocs(rdd, partition.index), rdd.partitions.indexOf(partition))
  }

}


object MiniStage {

  private[spark] def miniStages(stageId: Int, rdd: RDD[_], scheduler: DAGScheduler): Seq[MiniStage] = {
    // TODO(ryan): cache results because it's a DAG and not nec a tree
    rdd.dependencies flatMap {
      x => x match {
        case pipeline: PipelineDependency[_] =>
          Seq(new PipelineStage(stageId, pipeline.rdd, miniStages(stageId, pipeline.rdd, scheduler), scheduler))
        case miniFetch: MiniFetchDependency[_, _, _] =>
          Seq(new MiniFetchStage(stageId, rdd.asInstanceOf[MiniFetchRDD[_, _, _]], miniFetch, scheduler))
        case shuffle: ShuffleDependency[_, _, _] => Seq()
        case other: Dependency[_] => miniStages(stageId, other.rdd, scheduler)
      }
    }
  }

  /**
   * Do a BFS starting at the the MiniStage root and return a list of the tasks
   * in order of depth from the root
   * @return the sorted list of tasks and a map of the task to its MiniStage
   */
  def sortedTasks(root: MiniStage): (List[Task[_]], HashMap[Task[_], MiniStage]) = {
    val miniStageByTask = new mutable.HashMap[Task[_], MiniStage]()

    val bfsSorted = {
        val queue = new mutable.Queue[MiniStage]()
        val output = new mutable.Queue[Task[_]]()

        def bfs(stage: MiniStage) {
          val tasks: Seq[Task[_]] = stage.tasks
          for (task <- tasks) {
            miniStageByTask(task) = stage
            output.enqueue(task)
          }
          for (dep: MiniStage <- stage.dependencies) {
            queue.enqueue(dep)
          }
        }

        queue.enqueue(root)
        while (!queue.isEmpty) {
          bfs(queue.dequeue())
        }
        output
    }
    return (bfsSorted.toList, miniStageByTask)
  }


  /** Create a mini-stage from a final RDD. The MiniStage should hold ResultTasks */
  def resultFromFinalRDD(rdd: RDD[_], stageId: Int,
                        func: (TaskContext, scala.Iterator[_]) => (_$1) forSome {type _$1},
                          job: ActiveJob, scheduler: DAGScheduler): MiniStage = {
    new ResultStage(stageId, rdd, miniStages(stageId, rdd, scheduler), func, job, scheduler)
  }

  /** Create a mini-stage from a final RDD. The MiniStage should hold ShuffleMapTasks */
  def shuffleMapFromFinalRDD(rdd: RDD[_], stageId: Int, shuffleDependency: ShuffleDependency[_, _, _], scheduler: DAGScheduler) = {
    new ShuffleMapStage(stageId, rdd, miniStages(stageId, rdd, scheduler), shuffleDependency, scheduler)
  }

}

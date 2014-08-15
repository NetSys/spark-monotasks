package org.apache.spark.rdd

/**
 * Describes the what type of resource the RDD's compute function will use
 *
 * Compute primarily uses CPU,
 * Read uses disk if at a preferred location, else network
 * None uses nothing
 */
object RDDResourceTypes extends Enumeration {
  type RDDResourceTypes = Value
  val Compute, Read, None = Value
}

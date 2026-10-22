package com.tender.cloudberry.connector.reader

import org.apache.spark.sql.connector.read.InputPartition

/**
 * @param epoch         tender: the planned epoch of the scan (0 for a batch read, a micro-batch otherwise)
 * @param runSize       tender: number of partitions of the epoch (all of them run at once)
 * @param runPerAttempt tender: a part of a split read (read.split) — every task attempt is a run of its own
 *                      (a transfer of one task: a retry reads the part again, runSize 1)
 */
class CloudberryInputPartitionMeta(partitionNo: Int, locations: Array[String], val epoch: Long = 0, val runSize: Int = 1,
                                  val runPerAttempt: Boolean = false)
  extends InputPartition {
  override def preferredLocations: Array[String] = {
    locations
  }
  def getPartNo: Int = partitionNo
}

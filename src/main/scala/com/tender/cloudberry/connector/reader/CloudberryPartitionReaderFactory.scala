package com.tender.cloudberry.connector.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.tender.cloudberry.connector.CBOptionsFactory
import org.apache.spark.sql.types.StructType

/**
 * @param queryId            transfer session of the scan
 * @param coordinatorAddress tender: host:port of the driver transfer coordinator
 */
case class CloudberryPartitionReaderFactory (
                               optionsFactory: CBOptionsFactory,
                               queryId: String,
                               schema: StructType,
                               coordinatorAddress: String
                             )
  extends PartitionReaderFactory
{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new CloudberryInputPartitionReader(optionsFactory, queryId, schema,
      partition.asInstanceOf[CloudberryInputPartitionMeta], coordinatorAddress)
  }
}

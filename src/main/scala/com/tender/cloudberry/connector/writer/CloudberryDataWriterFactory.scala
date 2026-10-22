package com.tender.cloudberry.connector.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.tender.cloudberry.connector.CBOptionsFactory
import org.apache.spark.sql.types.StructType

/**
 * @param sessionId          tender: transfer session of the write on the driver
 * @param coordinatorAddress tender: host:port of the driver transfer coordinator
 * @param runSize            tender: number of write tasks (input partitions)
 * @param staging            tender: write.staging — every task attempt writes a staging table of its own
 */
class CloudberryDataWriterFactory(sessionId: String, schema: StructType,
                                 optionsFactory: CBOptionsFactory, coordinatorAddress: String, runSize: Int,
                                 staging: Boolean = false)
 extends DataWriterFactory
 with StreamingDataWriterFactory
{
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new CloudberryDataWriter(sessionId, schema, optionsFactory, coordinatorAddress, runSize, partitionId, taskId,
      staging = staging)
  }

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new CloudberryDataWriter(sessionId, schema, optionsFactory, coordinatorAddress, runSize, partitionId, taskId,
      Option(epochId), staging)
  }
}

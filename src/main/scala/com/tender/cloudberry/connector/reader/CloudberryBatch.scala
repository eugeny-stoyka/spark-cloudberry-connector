package com.tender.cloudberry.connector.reader

import com.tender.cloudberry.connector.CBOffset
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.tender.cloudberry.connector.CBOptionsFactory
import org.apache.spark.sql.types.StructType

class CloudberryBatch(optionsFactory: CBOptionsFactory,
                     queryId: String,
                     schema: StructType,
                     cloudberryScan: CloudberryScan
                     )
  extends Batch
  with MicroBatchStream
  with Logging
{
  override def planInputPartitions(): Array[InputPartition] = {
    cloudberryScan.planInputPartitions()
  }

  private var readerFactory: CloudberryPartitionReaderFactory = null

  override def createReaderFactory(): PartitionReaderFactory = this.synchronized {
    if (readerFactory == null)
      readerFactory = CloudberryPartitionReaderFactory(optionsFactory, queryId, schema, cloudberryScan.coordinatorAddress)
    readerFactory
  }

  override def latestOffset(): Offset = cloudberryScan.getEndOffset

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    cloudberryScan.setOffsetRange(Option[Offset](start), Option[Offset](end))
  }

  override def initialOffset(): Offset = cloudberryScan.getEndOffset

  override def deserializeOffset(json: String): Offset = CBOffset(Option(json))

  override def commit(end: Offset): Unit = cloudberryScan.commit(end)

  override def stop(): Unit = {
    cloudberryScan.stop()
    readerFactory = null
  }
}

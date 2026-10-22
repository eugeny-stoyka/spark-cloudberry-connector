package com.tender.cloudberry.connector.reader

import com.tender.cloudberry.connector.CBOffset
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.tender.cloudberry.connector.CBOptionsFactory
import org.apache.spark.sql.types.StructType

class CloudberryMicroBatch(optionsFactory: CBOptionsFactory,
                          queryId: String,
                          schema: StructType,
                          cloudberryScan: CloudberryScan)
  extends SupportsAdmissionControl
  with MicroBatchStream
  with Logging
{
  override def getDefaultReadLimit: ReadLimit = {
    logDebug("getDefaultReadLimit called")
    ReadLimit.maxRows(10)
  }

  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    cloudberryScan.getEndOffset
  }

  override def initialOffset(): Offset = cloudberryScan.getEndOffset

  override def deserializeOffset(json: String): Offset = CBOffset(Option(json))

  override def commit(end: Offset): Unit = cloudberryScan.commit(end)

  override def stop(): Unit = {
    cloudberryScan.stop()
    readerFactory = null
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    cloudberryScan.setOffsetRange(Option[Offset](start), Option[Offset](end))
  }

  private var readerFactory: CloudberryPartitionReaderFactory = null

  override def createReaderFactory(): PartitionReaderFactory = this.synchronized {
    if (readerFactory == null)
      readerFactory = CloudberryPartitionReaderFactory(optionsFactory, queryId, schema, cloudberryScan.coordinatorAddress)
    readerFactory
  }
}

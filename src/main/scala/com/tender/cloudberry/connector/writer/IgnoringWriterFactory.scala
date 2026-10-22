package com.tender.cloudberry.connector.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}

/** tender: saveMode=Ignore and the table exists — the rows are dropped */
private class IgnoringWriterFactory extends DataWriterFactory with StreamingDataWriterFactory {
  private def writer: DataWriter[InternalRow] = new DataWriter[InternalRow] {
    override def write(record: InternalRow): Unit = {}
    override def commit(): WriterCommitMessage = new WriterCommitMessage {}
    override def abort(): Unit = {}
    override def close(): Unit = {}
  }

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = writer

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = writer
}

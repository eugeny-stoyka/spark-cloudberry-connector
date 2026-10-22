package com.tender.cloudberry.connector.writer

import org.apache.spark.sql.connector.write.WriterCommitMessage

/** @param attempt tender: the task attempt — write.staging takes its staging table at the job commit */
case class CloudberryWriterCommitMessage(writeUUID : String, instanceId: String, gpfdistUrl: String,
                                        partitionId : Int, taskId : Long, epochId : Long,
                                        nRowsWritten : Long, rmiPutMs: Long, rmiGetMs: Long,
                                        nBytesWritten : Long, webTransferMs: Long, attempt: Int = 0
                                       )
  extends WriterCommitMessage
{}

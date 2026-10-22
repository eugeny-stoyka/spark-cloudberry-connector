package com.tender.cloudberry.connector.writer

import com.tender.cloudberry.connector.transfer.{CoordinatorClient, TaskChannel, TaskKey}
import com.tender.cloudberry.connector.utils.ProgressTracker
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.tender.cloudberry.connector.{CBOptionsFactory, SparkSchemaUtil, TextRowWriter}
import org.apache.spark.sql.types.StructType

/**
 * tender: a write task — its own gpfdist server ([[TaskChannel]]), started and registered at the first row.
 * commit() ends the data and waits for the INSERT of its batch. A task without rows (an empty partition
 * of the repartitioned input) starts nothing: commit() only tells the driver it is empty — no location
 * in an INSERT, no empty stream for Cloudberry.
 *
 * tender: the rows go into bytes ([[TextRowWriter]]) and to the channel in blocks of about
 * TextRowWriter.BlockBytes. Before: a String per row (internalRowToText, + LF, encoded into UTF-8), put into the
 * channel row by row, timed row by row (ProgressTracker) — ~60% of the time of the task.
 *
 * @param sessionId transfer session of the write (CloudberryBatchWrite)
 * @param runSize   number of write tasks (partitions)
 * @param staging   tender: write.staging — the task attempt is a run of its own (w/<partition>.<attempt>), its
 *                  rows go to its staging table
 */
class CloudberryDataWriter(sessionId: String, schema: StructType, optionsFactory: CBOptionsFactory,
                          coordinatorAddress: String, runSize: Int,
                          partitionId: Int, taskId: Long, batchId: Option[Long] = None, staging: Boolean = false)
  extends DataWriter[InternalRow] with Logging
{
  val epochId: Long = batchId.getOrElse(
    Option(TaskContext.get().getLocalProperty("streaming.sql.batchId")).getOrElse("0").toLong)
  private var rowCount: Long = 0
  private val instanceId: String = s"$partitionId:$taskId:$epochId"
  private val progressTracker: ProgressTracker = new ProgressTracker()
  private val schemaUtil = SparkSchemaUtil(optionsFactory.dbTimezone)
  private val rowWriter = new TextRowWriter(schema, schemaUtil)
  /** the rows in the block of rowWriter, not in the channel yet */
  private var blockRows: Long = 0
  private val attempt = TaskContext.get().attemptNumber()
  private val (run, taskRunSize) = if (staging) (s"w/$partitionId.$attempt", 1) else ("w", runSize)
  private var channel: TaskChannel = null // the first row starts it

  override def write(t: InternalRow): Unit = {
    rowWriter.write(t)
    if (channel == null)
      channel = new TaskChannel(optionsFactory, coordinatorAddress, sessionId, run, epochId,
        partitionId, attempt, taskRunSize, readTransfer = false)
    blockRows += 1
    rowCount += 1
    if (rowWriter.length >= TextRowWriter.BlockBytes)
      flushBlock()
  }

  /** the rows of the block into the channel */
  private def flushBlock(): Unit =
    if (blockRows > 0) {
      channel.writeRows(rowWriter.bytes, rowWriter.length, blockRows)
      rowWriter.reset()
      blockRows = 0
    }

  override def commit(): WriterCommitMessage = {
    if (channel == null) { // no rows: done, nothing to start
      new CoordinatorClient(coordinatorAddress, sessionId, TaskKey(run, partitionId, attempt), epochId, taskRunSize)
        .registerEmpty()
      logDebug(s"Write task $instanceId: empty")
      return CloudberryWriterCommitMessage(writeUUID = sessionId, instanceId = instanceId, gpfdistUrl = "",
        partitionId = partitionId, taskId = taskId, epochId = epochId,
        nRowsWritten = 0, rmiPutMs = 0, rmiGetMs = 0, nBytesWritten = 0, webTransferMs = 0, attempt = attempt)
    }
    val maxWait = if (optionsFactory.gpfdistTimeout > 0) optionsFactory.gpfdistTimeout else 0L
    flushBlock()
    channel.finish()
    progressTracker.trackProgress("commit") {
      channel.awaitDone(maxWait) // until the INSERT of the batch has read everything
    }
    val ret = CloudberryWriterCommitMessage(writeUUID = sessionId, instanceId = instanceId,
      gpfdistUrl = channel.gpfdistUrl, partitionId = partitionId, taskId = taskId, epochId = epochId,
      nRowsWritten = rowCount, rmiPutMs = channel.putWaitMs.get(), rmiGetMs = channel.getWaitMs.get(),
      nBytesWritten = channel.metrics.totalBytes.get(), webTransferMs = channel.metrics.processingMs.get(),
      attempt = attempt
    )
    logDebug(s"Write task $instanceId done: rows=$rowCount, ${progressTracker.reportTimeTaken()}")
    channel.close()
    ret
  }

  override def abort(): Unit = {
    rowWriter.reset()
    blockRows = 0
    if (channel != null)
      channel.abort(s"write task $instanceId aborted after $rowCount rows")
  }

  override def close(): Unit = {
    if (channel != null) {
      channel.close()
      channel = null
    }
  }
}

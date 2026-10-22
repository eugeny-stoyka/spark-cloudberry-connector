package com.tender.cloudberry.connector.reader

import com.tender.cloudberry.connector.transfer.TaskChannel
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.tender.cloudberry.connector.{CBOptionsFactory, SparkSchemaUtil, TextRowParser}
import org.apache.spark.sql.types.StructType

import java.io.IOException

// scalafix:off DisableSyntax.null
// scalastyle:off null

/**
 * tender: a read task — its own gpfdist server ([[TaskChannel]]) Cloudberry segments POST rows to. The run
 * of the task (to group the tasks of one execution on the driver) is the planned epoch + the stage attempt:
 * running the same scan again is a new run. A part of a split read (read.split) is a run of its own per
 * task attempt (+ the partition and the attempt): a retried task reads its part again.
 *
 * tender: a row per next() straight from the chunk of the channel ([[TextRowParser]], one row object); the chunk
 * goes back to the channel when its last row is taken. Before: all the rows of a chunk were parsed into a queue
 * (String, regex split, a row object per row) — 3/4 of the time of the task.
 */
class CloudberryInputPartitionReader(optionsFactory: CBOptionsFactory,
                                    queryId: String,
                                    schema: StructType,
                                    partition: CloudberryInputPartitionMeta,
                                    coordinatorAddress: String
                                    )
  extends PartitionReader[InternalRow] with Logging
{
  private val context = TaskContext.get()
  private val epochId: Long = partition.epoch
  private val run = s"${partition.epoch}/${context.stageId()}.${context.stageAttemptNumber()}" +
    (if (partition.runPerAttempt) s"/${partition.getPartNo}.${context.attemptNumber()}" else "")

  private val schemaUtil = SparkSchemaUtil(optionsFactory.dbTimezone)
  private val parser = new TextRowParser(schema, schemaUtil)
  /** a read of no columns (count): the rows of the placeholder column are not parsed, one row for all of them */
  private val placeholderRow: InternalRow =
    if (schema.isEmpty) schemaUtil.textToInternalRow(SparkSchemaUtil.getCloudberryPlaceholderSchema(optionsFactory), Array(""))
    else null
  private var dataLine: InternalRow = null
  private var rowCount: Long = 0
  private var channel: TaskChannel = new TaskChannel(optionsFactory, coordinatorAddress, queryId, run, epochId,
    partition.getPartNo, context.attemptNumber(), partition.runSize, readTransfer = true)
  private val gpfdistUrl: String = channel.gpfdistUrl
  logDebug(s"Read task ${partition.getPartNo} of run $run: gpfdist at $gpfdistUrl")

  private var eofReached: Boolean = false
  private var nextSliceNo: Int = 0
  /** the chunk being read: complete rows (LF-terminated) in data(0 until length), the next one at pos */
  private var chunk: java.nio.ByteBuffer = null
  private var chunkData: Array[Byte] = null
  private var chunkLength: Int = 0
  private var chunkPos: Int = 0

  override def next(): Boolean = this.synchronized {
    dataLine = null
    if (channel == null)
      return false
    while (chunkPos >= chunkLength) {
      if (chunk != null) {
        channel.recycle(chunk)
        chunk = null
      }
      val buff = channel.read() // waits for data
      if (buff == null) { // end of data
        eofReached = true
        logDebug(s"$gpfdistUrl epoch=$epochId end of stream, rowCount=$rowCount")
        closeInternal()
        return false
      }
      chunk = buff
      chunkData = buff.array()
      chunkLength = buff.position()
      chunkPos = 0
      nextSliceNo += 1
      if (chunkLength > 0 && chunkData(chunkLength - 1) != 0x0a) {
        logError(s"Unterminated row received after $rowCount rows")
        throw new IOException(s"Unterminated row received after $rowCount rows")
      }
    }
    if (placeholderRow != null) {
      while (chunkData(chunkPos) != 0x0a)
        chunkPos += 1
      chunkPos += 1
      dataLine = placeholderRow
    } else {
      dataLine = parser.parseLine(chunkData, chunkPos)
      chunkPos = parser.nextLine
    }
    true
  }

  override def get(): InternalRow = this.synchronized {
    if (dataLine == null) {
      logDebug(s"${gpfdistUrl} epoch=${epochId} get returns null, rowCount=$rowCount")
      return null
    }
    rowCount += 1
    dataLine
  }

  override def close(): Unit = synchronized {
    closeInternal()
  }

  private def closeInternal(): Unit = {
    if (channel == null)
      return
    if (chunk != null) {
      channel.recycle(chunk)
      chunk = null
    }
    if (eofReached)
      channel.close()
    else
      channel.discardRest() // stopped early (a limit, a cancel): do not break the INSERT of the other tasks
    logDebug(s"Destroyed $gpfdistUrl, epoch=$epochId, rowCount=$rowCount, slices=$nextSliceNo, " +
      s"waitMs=${channel.getWaitMs.get()}")
    channel = null
    chunkLength = 0
    chunkPos = 0
  }
}

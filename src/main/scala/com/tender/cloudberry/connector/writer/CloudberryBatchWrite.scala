package com.tender.cloudberry.connector.writer

import com.tender.cloudberry.connector.CBClient
import com.tender.cloudberry.connector.transfer.CBConnectorModes.CBConnectorMode
import com.tender.cloudberry.connector.transfer.{CBConnectorModes, TransferBatch, TransferCoordinator, TransferSession}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.tender.cloudberry.connector.{CBColumnMeta, CBOptionsFactory, CbTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.types.StructType

import java.sql.Connection
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

/**
 * tender: a write (or a micro-batch epoch of a streaming write) — one transaction on the driver connection.
 * Write tasks register their gpfdist servers with the [[TransferSession]] of the write; every batch of them
 * is one `INSERT ... SELECT FROM <readable external table with their locations>` in that transaction
 * (normally one batch: CloudberryWriteBuilder asks Spark for min(primary segments, task slots) partitions);
 * commit commits it, abort rolls it back (+ undo.side.effects.sql).
 *
 * Staging (write.staging, a batch write): every task attempt is a run of its own — on a connection of its own
 * it fills a staging table scc_stg_<session>_<partition>_<attempt> (LIKE the target, next to it) with its
 * rows; the job commit moves the staging tables of the committed attempts into the target in one transaction
 * on the driver connection (an overwrite with truncate — truncated in it, not before the write); the staging
 * tables of the session are dropped after the commit / the abort. A failed task writes only its part again,
 * speculation is allowed (the commit takes the committed attempt of every partition).
 *
 * @param truncateAtCommit tender: staging — the overwrite truncates the target in the commit transaction
 */
class CloudberryBatchWrite(writeUUID: String,
                          optionsFactory: CBOptionsFactory,
                          schema: StructType,
                          cbTableMeta: Map[String, CBColumnMeta],
                          targetTableCanonicalName: String,
                          cbClient: CBClient,
                          connectorMode: CBConnectorMode = CBConnectorModes.Batch,
                          ignoreInsert: Boolean = false,
                          truncateAtCommit: Boolean = false
                         )
  extends BatchWrite
  with StreamingWrite
  with Logging
{
  private val epochProcessingStart = new AtomicLong(System.currentTimeMillis())
  private val sessionId = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
  private val dbConnectionGuard = new Object()
  private var dbConnection: Connection = null
  private var session: TransferSession = null
  private var coordinatorAddress: String = null
  private var tempTableClause: String = "TEMP"
  private var runSize: Int = 0
  private val batchNo = new AtomicInteger(0)
  private val finished = new AtomicBoolean(false)
  private val sqlMs = new AtomicLong(0)

  private lazy val extTblCreateColumnsClause =
    SparkSchemaUtil.getCloudberryTableColumns(schema, CbTableTypes.ExternalReadable, cbTableMeta)
  private lazy val colListInsert = SparkSchemaUtil.getCloudberryTableColumns(schema, CbTableTypes.None)
  private lazy val colListSelect = SparkSchemaUtil.getCloudberrySelectColumns(schema, CbTableTypes.ExternalReadable, cbTableMeta)

  /** tender: write.staging applies — a batch write into a table (not a stream, not a sqlTransfer, not Ignore) */
  private val staging: Boolean = optionsFactory.writeStaging && connectorMode == CBConnectorModes.Batch &&
    optionsFactory.sqlTransfer.isEmpty && !ignoreInsert
  if (optionsFactory.writeStaging && !staging && !ignoreInsert)
    logWarning(s"write.staging is not applied to $writeUUID: ${if (connectorMode != CBConnectorModes.Batch) "a stream" else "a sqlTransfer write"}" +
      " — one transaction of all the write tasks")

  /** tender: the staging table of a task attempt: next to the target (its schema), scc_stg_<session>_<partition>_<attempt> */
  private def stagingTable(part: Int, attempt: Int): String = {
    val name = s"scc_stg_${sessionId}_${part}_$attempt"
    val dot = targetTableCanonicalName.lastIndexOf('.')
    if (dot > 0) targetTableCanonicalName.substring(0, dot + 1) + name else name
  }

  private def createWriterFactory(info: PhysicalWriteInfo): DataWriterFactory with StreamingDataWriterFactory =
    this.synchronized {
      if (finished.get()) {
        val msg = s"An attempt to revive an already finished action ${writeUUID}"
        logWarning(msg)
        throw new Exception(msg)
      }
      if (ignoreInsert) {
        logDebug(s"saveMode=Ignore and table ${targetTableCanonicalName} exists: rows are not written, " +
          s"writeUUID=${writeUUID}")
        return new IgnoringWriterFactory()
      }
      if (session == null) {
        if (schema.isEmpty)
          logWarning(s"Spark schema is empty for writeUUID=$writeUUID")
        runSize = info.numPartitions()
        dbConnection = cbClient.getConnection()
        if (!dbConnection.getAutoCommit)
          dbConnection.commit()
        dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        dbConnection.setAutoCommit(false)
        val nCbSegments = CBClient.queryNSegments(dbConnection)
        tempTableClause = if (optionsFactory.useTempExtTables) "TEMP" else ""
        // staging: every task attempt is a run of its own, all of them at once, a failure aborts that run only
        session = new TransferSession(sessionId, readTransfer = false, maxBatch = math.max(1, math.min(nCbSegments, optionsFactory.maxWriteSegments)),
          settleMs = optionsFactory.transferSettleMs, registerTimeoutMs = optionsFactory.networkTimeout,
          runBatch = runBatch, maxRunning = if (staging) Int.MaxValue else 1, isolatedRuns = staging)
        TransferCoordinator.register(session)
        coordinatorAddress = TransferCoordinator.address(optionsFactory.coordinatorPort)
        logDebug(s"Write $writeUUID (session $sessionId): $runSize tasks, $nCbSegments primary segments, " +
          s"coordinator $coordinatorAddress${if (staging) ", staging" else ""}")
      }
      new CloudberryDataWriterFactory(sessionId, schema, optionsFactory, coordinatorAddress, runSize, staging)
    }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    createWriterFactory(info)
  }
  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    createWriterFactory(info)
  }

  /**
   * The transfer SQL of a batch of write tasks (the session runs batches one after another; staging — a task
   * attempt into its staging table, all at once); the rows inserted for the row count check, None — a custom
   * sqltransfer (not comparable)
   */
  private def runBatch(batch: TransferBatch): Option[Long] =
    if (staging) runStagingBatch(batch)
    else dbConnectionGuard.synchronized {
      if (dbConnection == null)
        throw new IllegalStateException(s"writeUUID=$writeUUID is already finished")
      val insertSql: String => String = externalTableName =>
        if (optionsFactory.sqlTransfer.isEmpty) {
          s"insert into ${targetTableCanonicalName} (${colListInsert}) select ${colListSelect} from ${externalTableName}"
        } else {
          optionsFactory.sqlTransfer.
            replaceAll("(?i)<ext_table>", externalTableName).
            replaceAll("(?i)<current_epoch>", batch.epoch.toString)
        }
      val nRows = runTransfer(dbConnection, batch, insertSql)
      if (optionsFactory.sqlTransfer.isEmpty) Some(nRows.toLong) else None
    }

  /** tender: staging — the rows of a task attempt (the run w/<partition>.<attempt>) into its staging table */
  private def runStagingBatch(batch: TransferBatch): Option[Long] = {
    val Array(part, attempt) = batch.run.stripPrefix("w/").split('.').map(_.toInt)
    val table = stagingTable(part, attempt)
    val conn = CBClient.getConn(optionsFactory)
    try {
      conn.setAutoCommit(true)
      CBClient.executeStatement(conn, s"drop table if exists $table")
      CBClient.executeStatement(conn, s"create table $table (like $targetTableCanonicalName)")
      Some(runTransfer(conn, batch, externalTableName =>
        s"insert into $table ($colListInsert) select $colListSelect from $externalTableName").toLong)
    } finally {
      conn.close()
    }
  }

  /**
   * One transfer: a readable external table with the gpfdist locations of the tasks of the batch, the INSERT
   * from it (insertSql of the table name, cancellable — the session cancels the SQL of an aborted run), the
   * table dropped after; the rows of the INSERT
   */
  private def runTransfer(conn: Connection, batch: TransferBatch, insertSql: String => String): Int = {
    val start = System.currentTimeMillis()
    val externalTableName = s"ext${sessionId}_${batchNo.incrementAndGet()}"
    val locationClause = batch.urls.map(url => s"'$url'").mkString(", ")
    try {
      CBClient.executeStatement(conn, s"drop external table if exists $externalTableName")
      val createExtTbl = s"CREATE READABLE EXTERNAL $tempTableClause TABLE $externalTableName" +
        s"($extTblCreateColumnsClause)" +
        s" LOCATION ($locationClause)" +
        // tender: the default NULL \N (before: NULL 'NULL' — the string "NULL" was written as NULL)
        s" FORMAT 'TEXT' (" +
        s"    NEWLINE 'LF' " +
        s"  ) " +
        s" ENCODING 'UTF8'"
      logDebug(s"\n$createExtTbl")
      CBClient.executeStatement(conn, createExtTbl)
      val insertSelectSql = insertSql(externalTableName)
      logTrace(s"SQL: ${insertSelectSql}")
      val nRows = CBClient.executeStatement(conn, insertSelectSql, optionsFactory.dbMessageLogLevel,
        statement => batch.onCancel(() => statement.cancel()))
      logDebug(s"writeUUID=$writeUUID batch of ${batch.urls.size} tasks: '$insertSelectSql' nRows=$nRows")
      nRows
    } finally {
      try {
        CBClient.executeStatement(conn, s"drop external table if exists $externalTableName")
      } catch {
        case e: Exception => logDebug(s"drop external table $externalTableName: ${e.getMessage}")
      }
      sqlMs.addAndGet(System.currentTimeMillis() - start)
    }
  }

  /** tender: staging — the committed attempts into the target, one transaction on the driver connection */
  private def commitStaging(parts: Seq[CloudberryWriterCommitMessage]): Unit = dbConnectionGuard.synchronized {
    if (dbConnection == null)
      throw new Exception(s"writeUUID=${writeUUID} too late commit attempt")
    if (truncateAtCommit)
      CBClient.executeStatement(dbConnection, s"truncate table $targetTableCanonicalName")
    parts.filter(_.nRowsWritten > 0).foreach { part =>
      CBClient.executeStatement(dbConnection, s"insert into $targetTableCanonicalName ($colListInsert) " +
        s"select $colListInsert from ${stagingTable(part.partitionId, part.attempt)}")
    }
    dbConnection.commit()
  }

  /** tender: staging — drop the staging tables of the session (of failed attempts too), on a connection of its own */
  private def dropStagingTables(): Unit = {
    var conn: Connection = null
    try {
      conn = CBClient.getConn(optionsFactory)
      conn.setAutoCommit(true)
      val st = conn.createStatement()
      val tables = try {
        val rs = st.executeQuery("select quote_ident(schemaname) || '.' || quote_ident(tablename) from pg_tables " +
          s"where tablename like 'scc_stg_${sessionId}_%'")
        Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
      } finally st.close()
      tables.foreach(table => CBClient.executeStatement(conn, s"drop table if exists $table"))
      logDebug(s"writeUUID=$writeUUID: ${tables.size} staging tables dropped")
    } catch {
      case e: Exception => logWarning(s"writeUUID=$writeUUID: staging tables scc_stg_${sessionId}_* not dropped: $e")
    } finally {
      if (conn != null)
        scala.util.Try(conn.close())
    }
  }

  override def useCommitCoordinator: Boolean = false

  override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
    message match {
      case msg: CloudberryWriterCommitMessage =>
        logTrace(s"\nonDataWriterCommit writeUUID=${writeUUID}, " +
          s"instanceId=${msg.instanceId}, node=${msg.gpfdistUrl}, " +
          s"epochId=${msg.epochId}, partitionId=${msg.partitionId}, taskId=${msg.taskId}, " +
          s"nRowsWritten=${msg.nRowsWritten}, bufferPutMs=${msg.rmiPutMs}, waitMs=${msg.rmiGetMs}")
      case _ =>
        logTrace(s"\nwriteUUID=${writeUUID} onDataWriterCommit ${message.toString}")
    }
  }

  private def epochOf(writerCommitMessages: Array[WriterCommitMessage]): Long =
    writerCommitMessages.collectFirst { case part: CloudberryWriterCommitMessage => part.epochId }.getOrElse(0L)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit =
    commit(epochOf(writerCommitMessages), writerCommitMessages)

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit =
    abort(epochOf(writerCommitMessages), writerCommitMessages)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    try {
      val parts = messages.collect { case part: CloudberryWriterCommitMessage => part }
      if (staging) {
        commitStaging(parts) // every task has waited for its own staging table
      } else {
        if (session != null)
          session.awaitRunDone("w", runSize, optionsFactory.networkTimeout) // the tasks have waited for it already
        dbConnectionGuard.synchronized {
          if (dbConnection != null)
            dbConnection.commit()
          else if (!ignoreInsert)
            throw new Exception(s"writeUUID=${writeUUID} too late commit attempt, transaction is rolled back")
        }
      }
      val nParts = parts.length
      val processingMs = System.currentTimeMillis() - epochProcessingStart.get()
      logInfo(s"Epoch $epochId written ${parts.map(_.nRowsWritten).sum} rows in ${batchNo.get()} batches " +
        s"and $nParts parts, processingMs=$processingMs, sqlMs=${sqlMs.get()}, " +
        s"bufferPutMs/part=${if (nParts > 0) parts.map(_.rmiPutMs).sum.toDouble / nParts else 0.0}, " +
        s"totalBytes=${parts.map(_.nBytesWritten).sum}, webTransferMs=${parts.map(_.webTransferMs).sum}")
    } finally {
      close()
    }
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    logWarning(s"Abort epoch ${epochId}, batches=${batchNo.get()}, messages=\n${messages.mkString("{", "\n", "}")}")
    if (session != null)
      session.abort(s"write $writeUUID aborted") // the tasks stop serving, a running INSERT ends
    try {
      dbConnectionGuard.synchronized {
        if (dbConnection != null) {
          dbConnection.rollback()
          if (optionsFactory.undoSideEffectsSQL.nonEmpty) {
            CBClient.executeStatement(dbConnection, optionsFactory.undoSideEffectsSQL)
            dbConnection.commit()
          }
        }
      }
    } catch {
      case e: Exception => logWarning(s"On abort epoch ${epochId}: ${e}")
    } finally {
      close()
    }
  }

  private def close(): Unit = {
    finished.set(true)
    if (session != null)
      session.close()
    if (staging && session != null)
      dropStagingTables()
    dbConnectionGuard.synchronized {
      if (dbConnection != null) {
        try {
          dbConnection.close() // back to the pool (rolled back there if not committed)
        } catch {
          case e: Exception => logWarning(s"dbConnection.close(): ${e}")
        }
        dbConnection = null
      }
    }
  }
}

package com.tender.cloudberry.connector.writer

import com.tender.cloudberry.connector.CBClient
import com.tender.cloudberry.connector.transfer.CBConnectorModes
import com.tender.cloudberry.connector.transfer.CBConnectorModes.CBConnectorMode
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.connector.write.{BatchWrite, RequiresDistributionAndOrdering, SupportsOverwriteV2, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, SortOrder}
import org.apache.spark.sql.tender.cloudberry.connector.{CBColumnMeta, CBOptionsFactory, CBTarget, CbTableTypes, DirectTarget, DirectTransferTarget, SparkSchemaUtil}

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.sql.types.StructType

import java.sql.Connection
import org.apache.spark.sql.execution.streaming.AsyncProgressTrackingMicroBatchExecution

class CloudberryWriteBuilder(
                             cbClient: CBClient,
                             private var optionsFactory: CBOptionsFactory,
                             private var schema: StructType,
                             private var writeUUID: String
                           )
  extends WriteBuilder
    //with SupportsOverwriteV2
    //with SupportsOverwrite
    //with SupportsDynamicOverwrite
    with SupportsTruncate
    with Logging
{
  logDebug(s"\nPassed in properties = ${optionsFactory.dumpParams()}")
  private var dbTableName: String = optionsFactory.tableOrQuery
  private var saveMode: SaveMode = SaveMode.Append
  private var cbTableMeta: Map[String, CBColumnMeta] = null
  private var targetTableCanonicalName: String = ""
  private var ignoreInsert: Boolean = false
  private val initDone: AtomicBoolean = new AtomicBoolean(false)
  /** tender: write tasks = input partitions Spark is asked for (see write) */
  private var writeParallelism: Int = 0
  /** tender: write.staging + overwrite with truncate — the target is truncated in the commit transaction */
  private var truncateAtCommit: Boolean = false

  def update(newOptionsFactory: CBOptionsFactory, newSchema: StructType, newWriteUUID: String): Unit = {
    if ((newOptionsFactory != optionsFactory) ||
      (newSchema != schema) ||
      (newWriteUUID != writeUUID)
    ) {
      writeUUID = newWriteUUID
      schema = newSchema
      optionsFactory = newOptionsFactory
      initDone.set(false)
    }
  }

  private def buildDbObjects(): Unit = {
    if (!initDone.get()) {
      initDone.set(true)
      dbTableName = optionsFactory.tableOrQuery
      ignoreInsert = false
      truncateAtCommit = false
      var dbConnection: Connection = cbClient.getConnection()
      try {
        val dbDefaultSchemaName: String = CBClient.checkDbObjSearchPath(dbConnection, optionsFactory.dbSchema)
        if (!dbConnection.getAutoCommit)
          dbConnection.commit()
        dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        dbConnection.setAutoCommit(false)
        // tender: every write task has its own gpfdist location, Cloudberry allows no more locations than
        // primary segments (more tasks only make more batches — INSERTs one after another, no gain), and no
        // more than default_max_write_segments: by default as many tasks as that, but no more than the task
        // slots (so that they all run at once and the write is one INSERT); parallelism sets it explicitly
        val segmentsCap = math.min(CBClient.queryNSegments(dbConnection), optionsFactory.maxWriteSegments)
        writeParallelism = if (optionsFactory.parallelism > 0) {
          if (optionsFactory.parallelism > segmentsCap)
            logWarning(s"parallelism=${optionsFactory.parallelism} is above the write limit $segmentsCap (min of " +
              s"primary segments and default_max_write_segments=${optionsFactory.maxWriteSegments}), $segmentsCap is used")
          math.min(optionsFactory.parallelism, segmentsCap)
        } else math.max(1, math.min(segmentsCap, SparkSchemaUtil.totalTaskSlots()))
        val targetDetails: CBTarget = CBTarget(dbTableName)
        targetTableCanonicalName = targetDetails.getCanonicalName(dbDefaultSchemaName)
        val tableDbSchema = targetDetails.getTableSchema(dbDefaultSchemaName)
        /**
         * tender: CREATE TABLE of the target: before, the (clause, columns) pair of formatDistributedByClause was
         * appended as its toString ("(distributed by (id),id)") and partitionclause in parentheses — any
         * distributedby / partitionclause failed with a syntax error. storageclause (USING / WITH) goes before
         * DISTRIBUTED BY, partitionclause (the classic syntax) after it
         */
        def createTableSql(columnsClause: String): String = {
          val distributedBy = CBClient.formatDistributedByClause(optionsFactory.distributedBy)._1
          s"create table ${targetTableCanonicalName} ($columnsClause)" +
            (if (optionsFactory.storageClause.nonEmpty) s" ${optionsFactory.storageClause}" else "") +
            (if (distributedBy.nonEmpty) s" $distributedBy" else "") +
            (if (optionsFactory.partitionClause.nonEmpty) s" ${optionsFactory.partitionClause}" else "")
        }
        if (targetTableCanonicalName.nonEmpty) {
          if (!CBClient.tableExists(dbConnection, targetTableCanonicalName)) {
            val createColumnsClause = SparkSchemaUtil.getCloudberryTableColumns(schema, CbTableTypes.Target)
            CBClient.executeStatement(dbConnection, createTableSql(createColumnsClause))
            cbTableMeta = SparkSchemaUtil.getCloudberryTableMeta(dbConnection, tableDbSchema, targetDetails.cleanDbTableName)
          } else {
            cbTableMeta = SparkSchemaUtil.getCloudberryTableMeta(dbConnection, tableDbSchema, targetDetails.cleanDbTableName)
            if (saveMode == SaveMode.ErrorIfExists) {
              throw new Exception(s"table ${targetTableCanonicalName} exists and saveMode=ErrorIfExists")
            } else if (saveMode == SaveMode.Ignore) {
              ignoreInsert = true
            } else if (saveMode == SaveMode.Overwrite) {
              logDebug(s"saveMode = overwrite, truncate = ${optionsFactory.truncate}")
              if (optionsFactory.truncate && stagingAsked) {
                truncateAtCommit = true // tender: in the commit transaction — a failed write keeps the old rows
              } else if (optionsFactory.truncate) {
                logTrace(s"truncate table ${targetTableCanonicalName}")
                CBClient.executeStatement(dbConnection, s"truncate table ${targetTableCanonicalName}")
              } else {
                val createColumnsClause = SparkSchemaUtil.getCloudberryTableColumns(schema, CbTableTypes.Target, cbTableMeta)
                CBClient.executeStatement(dbConnection, s"drop table ${targetTableCanonicalName}")
                CBClient.executeStatement(dbConnection, createTableSql(createColumnsClause))
              }
            } else {
              logDebug(s"saveMode = append")
            }
          }
          if (!dbConnection.getAutoCommit)
            dbConnection.commit()
        } else if (optionsFactory.sqlTransfer.isEmpty) {
          throw new IllegalArgumentException(s"Must specify either dbtable or sqltransfer option, or both")
        }
      } finally {
        dbConnection.close()
        dbConnection = null
      }
    } else {
      logDebug(s"writeUUID=${writeUUID} buildDbObjects bypass")
    }
  }

  /** tender: write.staging asked for a table write (CloudberryBatchWrite applies it to a batch write only) */
  private def stagingAsked: Boolean = optionsFactory.writeStaging && optionsFactory.sqlTransfer.isEmpty

  private def newBatch(connectorMode: CBConnectorMode = CBConnectorModes.Batch): CloudberryBatchWrite = {
    new CloudberryBatchWrite(writeUUID, optionsFactory, schema,
      cbTableMeta, targetTableCanonicalName, cbClient, connectorMode, ignoreInsert, truncateAtCommit)
  }

  private def write: Write = new Write with RequiresDistributionAndOrdering with DirectTransferTarget {
    override def toBatch: BatchWrite = newBatch()
    // tender: write.direct — the target as build() has prepared it
    override def directTarget: DirectTarget = DirectTarget(optionsFactory, () => cbClient.getConnection(), schema,
      cbTableMeta, targetTableCanonicalName, ignoreInsert, truncateAtCommit)
    override def toStreaming: StreamingWrite = {
      // tender: a stream writes without staging: the checks build() left to the batch write
      if (stagingAsked) {
        SparkSchemaUtil.requireNoSpeculation("write")
        if (truncateAtCommit) {
          val conn = cbClient.getConnection()
          try {
            conn.setAutoCommit(true)
            CBClient.executeStatement(conn, s"truncate table ${targetTableCanonicalName}")
          } finally conn.close()
          truncateAtCommit = false
        }
      }
      newBatch(CBConnectorModes.MicroBatch)
    }

    // tender: Spark repartitions the input into writeParallelism partitions (write tasks, gpfdist
    // locations) by the hash of the columns; write.repartition=false — a write task per input partition
    private val clustering: Array[Expression] =
      if (writeParallelism > 0 && optionsFactory.writeRepartition) SparkSchemaUtil.hashClusteringColumns(schema)
      else Array.empty
    if (writeParallelism > 0 && optionsFactory.writeRepartition && clustering.isEmpty)
      logWarning(s"write parallelism $writeParallelism is not applied: no columns to repartition by (all hold maps)")
    if (!optionsFactory.writeRepartition && optionsFactory.parallelism > 0)
      logWarning(s"parallelism=${optionsFactory.parallelism} is not applied with write.repartition=false: " +
        s"a write task per input partition")

    override def requiredDistribution(): Distribution =
      if (clustering.nonEmpty) Distributions.clustered(clustering) else Distributions.unspecified()

    override def requiredNumPartitions(): Int = if (clustering.nonEmpty) writeParallelism else 0

    override def requiredOrdering(): Array[SortOrder] = Array.empty
  }

  override def build(): Write = {
    // tender: before the target table is created / dropped / truncated; a staging write is safe with
    // speculation — a copy of a task writes a staging table of its own
    if (!stagingAsked)
      SparkSchemaUtil.requireNoSpeculation("write")
    buildDbObjects()
    write
  }

  override def truncate(): WriteBuilder = {
    logDebug("truncate called")
    saveMode = SaveMode.Overwrite
    this
  }
}

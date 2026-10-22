package com.tender.cloudberry.connector

import com.tender.cloudberry.connector.reader.CloudberryScanBuilder
import com.tender.cloudberry.connector.writer.CloudberryWriteBuilder
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.tender.cloudberry.connector.{CBOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.Connection
import scala.collection.JavaConverters._
import java.util

class CloudberryRowSet(inputSchema: StructType, passedInPartitioning: Array[Transform], passedInProperties: util.Map[String, String])
  extends Table
  with SupportsRead
  with SupportsWrite
  with Logging
{
  private val optionsFactory = initOptions()
  logDebug(s"\npassedInProperties=${optionsFactory.dumpParams()}")
  private var cbClient: CBClient = null
  private var actualSchema: StructType = initSchema()

  def clear(): Unit = {
    cbClient = null
    actualSchema = null
  }

  def getCbClient: CBClient = {
    if (cbClient == null) cbClient = new CBClient(optionsFactory)
    cbClient
  }
  private def initOptions(): CBOptionsFactory = {
    val sparkContext = SparkContext.getOrCreate
    val sparkConf = sparkContext.getConf.getAll.toMap.filter(p =>
      p._1 == "spark.executor.heartbeatInterval"
        || p._1 == "spark.files.fetchTimeout"
        || p._1 == "spark.network.timeout"
    )
    CBOptionsFactory(passedInProperties.asScala.toMap ++ sparkConf)
  }

  private def initSchema(): StructType = {
    var ret = inputSchema
    val dbConnection: Connection = getCbClient.getConnection()
    try {
      CBClient.checkServer(dbConnection, optionsFactory.url)
      CBClient.checkDbObjSearchPath(dbConnection, optionsFactory.dbSchema)
      if ((inputSchema == null) || (inputSchema.isEmpty)) {
        if (dbConnection.getAutoCommit) dbConnection.commit()
        dbConnection.setAutoCommit(true)
        ret = SparkSchemaUtil.getCloudberryTableSchema(optionsFactory, dbConnection, optionsFactory.tableOrQuery)
      }
    } finally {
      dbConnection.close() // tender: back to the shared pool also on errors
    }
    ret
  }

  logDebug(s"passedInPartitioning.size=${passedInPartitioning.length}")
  logDebug(s"inputSchema = \n${if (inputSchema != null) inputSchema.treeString else "null"}")
  logDebug(s"actualSchema = \n${actualSchema.treeString}")
  //logDebug((s"passedInProperties=${Json.toJson(passedInProperties.asScala.toMap)}"))

/*
For asynchronous progress tracking name must be one of:
  "noop-table"
  "console"
  "MemorySink"
  "KafkaTable"
  See org.apache.spark.sql.execution.streaming.AsyncProgressTrackingMicroBatchExecution.validateAndGetTrigger
*/
  override def name(): String = optionsFactory.actionName

  override def schema(): StructType = {
    if (actualSchema == null) actualSchema = initSchema()
    actualSchema.copy()
  }
  override def columns(): Array[Column] = SparkSchemaUtil.schema2Columns(actualSchema)

  override def partitioning(): Array[Transform] = passedInPartitioning

  override def properties(): util.Map[String, String] = passedInProperties

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.MICRO_BATCH_READ,
    TableCapability.STREAMING_WRITE,
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.TRUNCATE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val optionsUnion = optionsFactory.params ++ options.asScala
    val opts = CBOptionsFactory(optionsUnion)
    new CloudberryScanBuilder(opts,
      this,
      schema()
      )
  }

  private var writeBuilder: CloudberryWriteBuilder = null
  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = this.synchronized {
    if (schema() != logicalWriteInfo.schema()) {
      logDebug(s"WriteId ${logicalWriteInfo.queryId()} schema changed from ${actualSchema} " +
        s"to ${logicalWriteInfo.schema()}")
      actualSchema = logicalWriteInfo.schema()
    }
    val optionsUnion = optionsFactory.params ++ logicalWriteInfo.options().asScala
    val opts = CBOptionsFactory(optionsUnion)
    val queryId = logicalWriteInfo.queryId()
    if (writeBuilder == null) {
      writeBuilder = new CloudberryWriteBuilder(getCbClient, opts, schema(), queryId)
    } else {
      writeBuilder.update(opts, schema(), queryId)
    }
    writeBuilder
  }
}

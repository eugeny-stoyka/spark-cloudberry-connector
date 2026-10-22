package com.tender.cloudberry.connector.reader

import com.tender.cloudberry.connector.transfer.CBConnectorModes.CBConnectorMode
import com.tender.cloudberry.connector.{CBClient, CBOffset, CloudberryRowSet}
import com.tender.cloudberry.connector.transfer.{CBConnectorModes, NetUtils, TransferBatch, TransferCoordinator, TransferSession}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, Scan, Statistics, SupportsReportPartitioning, SupportsReportStatistics}
import org.apache.spark.sql.tender.cloudberry.connector.{CBColumnMeta, CBOptionsFactory, CBTarget, CbTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, IntegerType, LongType, ShortType, TimestampNTZType, TimestampType}

import java.sql.Connection
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.{OptionalLong, UUID}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.sql.tender.cloudberry.connector.SparkSchemaUtil.totalTaskSlots

/**
 * tender: a read of a Cloudberry table / query, batch or micro-batch.
 *
 * Every planned epoch (the batch read — epoch 0, each micro-batch offset range — the next one) has
 * min(primary segments, task slots, parallelism) partitions. Read tasks register their gpfdist servers with
 * the [[TransferSession]] of the scan; when all tasks of a run (an execution of the epoch stage) are there,
 * the session runs `INSERT INTO <writable external table with their locations> SELECT ...` on the scan
 * connection — Cloudberry segments push the rows to the tasks — with the offsets of that epoch.
 *
 * Split read (read.split=partitions, a batch read of a partitioned table): the leaf partitions are grouped
 * into parts of about the same size (catalog statistics), a Spark partition per part. A part is a run of
 * its own per task attempt and its own `INSERT INTO <writable external table with the one location of the
 * task> SELECT ... FROM leaf1 UNION ALL ...` on a connection of its own; the parts run at once. A failed
 * task reads only its part again, a speculative copy reads it too (the first one wins). Every part is
 * consistent in itself, the parts are separate transactions — not one snapshot.
 */
class CloudberryScan(optionsFactory: CBOptionsFactory,
                    rowSet: CloudberryRowSet,
                    schema: StructType,
                    whereClause: String
                   )
  extends Scan
    with SupportsReportPartitioning
    with SupportsReportStatistics
    with Logging
{
  private var connectorMode: CBConnectorMode = CBConnectorModes.Batch
  private val dbConnectionGuard: AnyRef = new Object
  private var dbConnection: Connection = cbConnect(true)
  dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)

  private val queryId: String = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
  private val tableOrQuery: String = optionsFactory.tableOrQuery
  if (tableOrQuery.isEmpty && schema.isEmpty)
    throw new IllegalArgumentException(s"Must pass a source table name or sql query string in the dbtable option")
  private val targetDetails: CBTarget = CBTarget(tableOrQuery)
  private val dbDefaultSchemaName: String = CBClient.checkDbObjSearchPath(dbConnection, optionsFactory.dbSchema)
  private val dbTableName: String = targetDetails.getCanonicalName(dbDefaultSchemaName)
  private val startOffset: CBOffset = CBOffset()
  startOffset.extract(dbConnection, optionsFactory.offsetRestoreSql)
  private val endOffset: CBOffset = CBOffset(startOffset.get())
  private val useTempExtTables: Boolean = optionsFactory.useTempExtTables
  private val tempTableClause: String = if (useTempExtTables) "TEMP" else ""
  private val distributionPol: String = CBClient.getTableDistributionPolicy(dbConnection,
    targetDetails.getTableSchema(dbDefaultSchemaName),
    targetDetails.cleanDbTableName)
  private val (distributedByClause: String, distributionColNames: String) = CBClient.formatDistributedByClause(
    if (optionsFactory.distributedBy.nonEmpty) optionsFactory.distributedBy
    else distributionPol
  )
  /** tender: (rows, bytes) of the table from the catalog; None — a query, a sqlTransfer, never analyzed */
  private val tableStats: Option[(Long, Long)] =
    if (dbTableName.nonEmpty && optionsFactory.sqlTransfer.isEmpty) CBClient.queryTableStats(dbConnection, dbTableName)
    else None
  /**
   * tender: read.split=partitions — the parts of the read (qualified leaf partitions); empty — one transfer
   * per run (not asked for, or not applicable — with a warning)
   */
  private val splitParts: IndexedSeq[SplitPart] = planSplit(dbConnection)
  /**
   * tender: the Cloudberry types of the source columns, for the casts of the read (bit(1) as boolean, time /
   * timetz as timestamp — Spark JDBC types them so); only when the schema has such columns
   */
  private val sourceMeta: Map[String, CBColumnMeta] =
    if (tableOrQuery.isEmpty || !schema.exists(f => Seq(BooleanType, TimestampType, TimestampNTZType).contains(f.dataType))) Map.empty
    else scala.util.Try(SparkSchemaUtil.getSourceColumnMeta(dbConnection, if (targetDetails.isQuery) tableOrQuery else dbTableName))
      .recover { case e => logDebug(s"no column types of $tableOrQuery: ${e.getMessage}"); Map.empty[String, CBColumnMeta] }.get
  dbConnectionGuard.synchronized {
    if (!dbConnection.getAutoCommit)
      dbConnection.commit()
    dbConnection.close()
    dbConnection = null
  }
  logDebug(s"distributionPol=$distributionPol,\n" +
    s"tblSchema=${targetDetails.getTableSchema(dbDefaultSchemaName)},\n" +
    s"distributedByClause=$distributedByClause,\n")
  // tender: a split read is safe with speculation — a copy of a task is a run of its own
  if (splitParts.isEmpty)
    SparkSchemaUtil.requireNoSpeculation("read")

  /** tender: where the read tasks reach the transfer coordinator of the driver */
  val coordinatorAddress: String = TransferCoordinator.address(optionsFactory.coordinatorPort)
  private val runNo = new AtomicLong(0)
  private val session = new TransferSession(queryId, readTransfer = true, maxBatch = Int.MaxValue,
    settleMs = optionsFactory.transferSettleMs, registerTimeoutMs = optionsFactory.networkTimeout, runBatch = runEpoch,
    maxRunning = if (splitParts.nonEmpty) Int.MaxValue else 1)
  TransferCoordinator.register(session)

  override def readSchema(): StructType = schema

  private val toBatchCallNo: AtomicLong = new AtomicLong(0)

  private def cbConnect(autoCommit: Boolean = false): Connection = {
    dbConnectionGuard.synchronized {
      if (dbConnection == null) {
        dbConnection = rowSet.getCbClient.getConnection()
        if (autoCommit || (connectorMode == CBConnectorModes.Batch) || optionsFactory.readStreamAutoCommit) {
          dbConnection.setAutoCommit(true)
        } else {
          dbConnection.setAutoCommit(false)
        }
      }
    }
    dbConnection
  }

  /** The scan connection (opened if closed — commit(end) of a micro-batch closes it), under its guard */
  private def withConnection[T](f: Connection => T): T = dbConnectionGuard.synchronized {
    f(cbConnect())
  }

  /**
   * min(primary segments, default_max_read_segments, task slots, parallelism) partitions.
   * tender: task slots, not executors — every read task has its own gpfdist server now (before: one per
   * executor), so 8 executors x 4 cores read with 32 tasks, not 8
   */
  private def getNPartsAndLocations(): (Int, Map[String, Set[String]]) = withConnection { conn =>
    val segmentsCap = math.min(CBClient.queryNSegments(conn), optionsFactory.maxReadSegments)
    if (optionsFactory.parallelism > segmentsCap)
      logWarning(s"parallelism=${optionsFactory.parallelism} is above the read limit $segmentsCap " +
        s"(min of primary segments and default_max_read_segments=${optionsFactory.maxReadSegments}), $segmentsCap is used")
    val numParts = optionsFactory.limitParallelism(math.min(segmentsCap, totalTaskSlots()))
    val hosts = CBClient.nodeNamesWithSegments(conn)
    val resolvedHosts = hosts.map({ case (host, segSet) => NetUtils().resolveHost2Ip(host) -> segSet })
    (math.max(1, numParts), hosts ++ resolvedHosts)
  }

  // ---------- planning: an epoch per batch read / micro-batch offset range ----------

  private val planGuard = new Object
  private var plannedEpoch: Long = -1
  private var plannedRange: (Option[String], Option[String]) = (None, None)
  private val epochOffsets = new ConcurrentHashMap[Long, (String, String)]()
  private var inputPartitions: Array[InputPartition] = Array.empty

  private def planEpoch(epoch: Long): Unit = {
    epochOffsets.put(epoch, (startOffset.json(), endOffset.json()))
    if (splitActive) {
      // a part: a run of its own per task attempt; locality — the hosts of its segments (a segments split)
      inputPartitions = splitParts.indices.map { p =>
        new CloudberryInputPartitionMeta(p, splitParts(p).locations, epoch, runSize = 1, runPerAttempt = true): InputPartition
      }.toArray
    } else {
      val (numParts, segmentLocations) = getNPartsAndLocations()
      inputPartitions = (0 until numParts).map { i =>
        val locations: Array[String] = segmentLocations.filter(node => node._2.contains(i.toString)).keySet.toArray
        new CloudberryInputPartitionMeta(i, locations, epoch, numParts): InputPartition
      }.toArray
    }
    plannedEpoch = epoch
    plannedRange = (startOffset.get(), endOffset.get())
    logDebug(s"Scan $queryId: ${inputPartitions.length} partitions for epoch $epoch")
  }

  /** tender: read.split applies — a batch read with parts planned (a stream reads every epoch as one transfer) */
  private def splitActive: Boolean = splitParts.nonEmpty && connectorMode == CBConnectorModes.Batch

  /** tender: read.split — the parts of the read; empty — not asked for or not applicable (a warning) */
  private def planSplit(conn: Connection): IndexedSeq[SplitPart] = {
    val mode = optionsFactory.readSplit
    if (mode == "none")
      return IndexedSeq.empty
    val (parts, reason) = mode match {
      case _ if optionsFactory.sqlTransfer.nonEmpty => (IndexedSeq.empty, "a sqlTransfer read")
      case "partitions" => planPartitionsSplit(conn)
      case "segments" => planSegmentsSplit(conn)
      case "column" => planColumnSplit()
    }
    if (parts.isEmpty)
      logWarning(s"read.split=$mode is not applied to $tableOrQuery: $reason; it is read as one transfer")
    else
      logInfo(s"Scan $queryId: read.split=$mode of $tableOrQuery in ${parts.size} parts; $reason")
    parts
  }

  /**
   * partitions — the leaf partitions of the table into parts of about the same size by the catalog (the
   * largest leaves first, each into the smallest part; never analyzed ones count as an average leaf):
   * `read.split.parts`, by default 4 x task slots, at most the leaves
   * @return the parts (empty — not applicable) and the reason / a summary
   */
  private def planPartitionsSplit(conn: Connection): (IndexedSeq[SplitPart], String) = {
    if (dbTableName.isEmpty)
      return (IndexedSeq.empty, "not a table (a query)")
    val leaves = CBClient.queryLeafPartitions(conn, dbTableName)
    if (leaves.isEmpty)
      return (IndexedSeq.empty, "not a partitioned table")
    val denied = leaves.filterNot(_._3)
    if (denied.nonEmpty)
      return (IndexedSeq.empty, s"no SELECT privilege on ${denied.size} of its ${leaves.size} leaf partitions (${denied.head._1})")
    val nParts = math.min(leaves.size,
      if (optionsFactory.readSplitParts > 0) optionsFactory.readSplitParts else 4 * totalTaskSlots())
    val known = leaves.map(_._2).filter(_ > 0)
    val averageLeaf = if (known.nonEmpty) known.sum / known.size else 1L
    val parts = Array.fill(nParts)(scala.collection.mutable.ArrayBuffer[String]())
    val sizes = Array.fill(nParts)(0L)
    leaves.map { case (leaf, bytes, _) => (leaf, if (bytes > 0) bytes else averageLeaf) }.sortBy(-_._2).foreach {
      case (leaf, bytes) =>
        val p = sizes.indices.minBy(sizes(_))
        parts(p) += leaf
        sizes(p) += bytes
    }
    (parts.map(leaves => SplitPart(leaves.sorted.toSeq)).toIndexedSeq,
      s"${leaves.size} leaf partitions, ${sizes.min}..${sizes.max} bytes per part by the catalog")
  }

  /**
   * segments — a part per primary segment (`read.split.parts` fewer: segments grouped), `WHERE gp_segment_id
   * = N`: Cloudberry must dispatch such a query to that segment only (Cloudberry 2 does, GPORCA and the
   * Postgres planner alike; EXPLAIN is checked) — otherwise every part would scan the whole table. Not for a
   * replicated table (every segment holds all the rows).
   */
  private def planSegmentsSplit(conn: Connection): (IndexedSeq[SplitPart], String) = {
    if (dbTableName.isEmpty)
      return (IndexedSeq.empty, "not a table (a query)")
    if (distributionPol.toLowerCase.contains("replicated"))
      return (IndexedSeq.empty, "a replicated table: every segment holds all its rows")
    val segments = CBClient.queryNSegments(conn)
    val nParts = math.min(segments, if (optionsFactory.readSplitParts > 0) optionsFactory.readSplitParts else segments)
    if (nParts < 2)
      return (IndexedSeq.empty, s"$segments primary segment(s)")
    val groups = (0 until nParts).map(p => (0 until segments).filter(_ % nParts == p))
    def condition(segs: Seq[Int]): String =
      if (segs.size == 1) s"gp_segment_id = ${segs.head}" else s"gp_segment_id in (${segs.mkString(", ")})"
    val dispatched = CBClient.dispatchedSegments(conn, s"select 1 from $dbTableName where ${condition(groups.head)}")
    if (!dispatched.contains(groups.head.size))
      return (IndexedSeq.empty, s"Cloudberry dispatches `where ${condition(groups.head)}` to " +
        s"${dispatched.map(n => s"$n segments").getOrElse("all segments")} (EXPLAIN, not ${groups.head.size}): " +
        "every part would scan the whole table")
    val hosts = CBClient.nodeNamesWithSegments(conn)
    val allHosts = hosts ++ hosts.map { case (host, segs) => NetUtils().resolveHost2Ip(host) -> segs }
    (groups.map(segs => SplitPart(Seq(dbTableName), condition(segs),
      allHosts.filter { case (_, hostSegs) => segs.exists(s => hostSegs.contains(s.toString)) }.keySet.toArray)),
      s"$segments primary segments, ${groups.map(_.size).max} per part")
  }

  /**
   * column — ranges of `read.split.column` (integral, date, timestamp) between `read.split.lowerBound` and
   * `read.split.upperBound`, as partitionColumn of Spark JDBC: equal strides, the first part also below the
   * lower bound and NULL, the last — above the upper one. Efficient when Cloudberry prunes by the column (the
   * partition key) or has an index on it — otherwise every part scans the whole table.
   */
  private def planColumnSplit(): (IndexedSeq[SplitPart], String) = {
    val column = optionsFactory.readSplitColumn
    if (column.isEmpty || optionsFactory.readSplitLowerBound.isEmpty || optionsFactory.readSplitUpperBound.isEmpty)
      throw new IllegalArgumentException("read.split=column needs read.split.column, read.split.lowerBound and read.split.upperBound")
    val field = rowSet.schema().fields.find(_.name.equalsIgnoreCase(column.stripPrefix("\"").stripSuffix("\""))).getOrElse(
      throw new IllegalArgumentException(s"read.split.column: no column $column in $tableOrQuery"))
    // the bounds as longs (days of a date, microseconds of a timestamp) and back as SQL literals
    val (toLong, literal): (String => Long, Long => String) = field.dataType match {
      case ByteType | ShortType | IntegerType | LongType => (_.toLong, _.toString)
      case DateType => (s => LocalDate.parse(s).toEpochDay, d => s"date '${LocalDate.ofEpochDay(d)}'")
      case TimestampType => (s => {
        val t = LocalDateTime.parse(s.replace(' ', 'T'))
        t.toEpochSecond(ZoneOffset.UTC) * 1000000L + t.getNano / 1000
      }, us => s"timestamp '${LocalDateTime.ofEpochSecond(Math.floorDiv(us, 1000000L),
        (Math.floorMod(us, 1000000L) * 1000).toInt, ZoneOffset.UTC).toString.replace('T', ' ')}'")
      case t => throw new IllegalArgumentException(
        s"read.split.column $column is ${t.catalogString}: an integral, date or timestamp column is needed")
    }
    val (lower, upper) = (toLong(optionsFactory.readSplitLowerBound), toLong(optionsFactory.readSplitUpperBound))
    if (lower >= upper)
      throw new IllegalArgumentException(s"read.split.lowerBound must be below read.split.upperBound: $lower >= $upper")
    val nParts = math.max(1L, math.min(upper - lower,
      if (optionsFactory.readSplitParts > 0) optionsFactory.readSplitParts else 4L * totalTaskSlots())).toInt
    if (nParts < 2)
      return (IndexedSeq.empty, "one part")
    val stride = upper / nParts - lower / nParts
    val source = if (dbTableName.nonEmpty) dbTableName else s"($tableOrQuery) sqr"
    val parts = (0 until nParts).map { i =>
      val from = lower + i * stride
      val conditions = Seq(
        if (i > 0) s"$column >= ${literal(from)}" else "",
        if (i < nParts - 1) s"$column < ${literal(from + stride)}" else "").filter(_.nonEmpty)
      SplitPart(Seq(source), if (i == 0) s"${conditions.mkString(" and ")} or $column is null" else conditions.mkString(" and "))
    }
    (parts, s"$column from ${literal(lower)} to ${literal(upper)}, stride $stride")
  }

  /** Batch read: epoch 0 (the same scan executed again is a new run of it, see CloudberryInputPartitionReader) */
  def planInputPartitions(): Array[InputPartition] = planGuard.synchronized {
    if (plannedEpoch < 0)
      planEpoch(0)
    inputPartitions
  }

  /** Micro-batch: a new epoch for a new offset range */
  def setOffsetRange(start: Option[Offset], end: Option[Offset]): Array[InputPartition] = planGuard.synchronized {
    start.collect { case offset: CBOffset => startOffset.set(offset.get()) }
    end.collect { case offset: CBOffset => endOffset.set(offset.get()) }
    if (plannedEpoch < 0 || plannedRange != ((startOffset.get(), endOffset.get())))
      planEpoch(plannedEpoch + 1)
    inputPartitions
  }

  override def outputPartitioning(): Partitioning = new UnknownPartitioning(planInputPartitions().length)

  /**
   * tender: the catalog statistics of the table and its partitions (an upper bound with pushed filters);
   * unknown for a query, a sqlTransfer or a never analyzed table — Spark then takes
   * spark.sql.defaultSizeInBytes and never broadcasts it. Before: 2000 bytes / 20 rows for any source, so a
   * large table in a join was broadcast (collected to the driver).
   */
  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = tableStats.map(s => OptionalLong.of(s._2)).getOrElse(OptionalLong.empty())
    override def numRows(): OptionalLong =
      tableStats.map(_._1).filter(_ > 0).map(n => OptionalLong.of(n)).getOrElse(OptionalLong.empty())
  }

  private var batch: CloudberryBatch = null
  override def toBatch: Batch = {
    toBatchCallNo.incrementAndGet()
    if (toBatchCallNo.get() == 1) {
      connectorMode = CBConnectorModes.Batch
      batch = new CloudberryBatch(optionsFactory, queryId, schema, this)
    }
    logDebug(s"toBatch callNo=${toBatchCallNo.get()}")
    batch
  }

  private var microBatch: CloudberryMicroBatch = null
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    toBatchCallNo.incrementAndGet()
    if (toBatchCallNo.get() == 1) {
      connectorMode = CBConnectorModes.MicroBatch
      if (splitParts.nonEmpty) { // the constructor skipped the check for the split read
        logWarning(s"read.split is for batch reads: the stream of $tableOrQuery reads every micro-batch as one transfer")
        SparkSchemaUtil.requireNoSpeculation("read")
      }
      microBatch = new CloudberryMicroBatch(optionsFactory, queryId, schema, this)
    }
    logDebug(s"toMicroBatchStream callNo=${toBatchCallNo.get()}")
    microBatch
  }

  // ---------- the transfer SQL of a run (the session calls it when all read tasks are registered) ----------

  /** The columns of the transfer: the schema, or a placeholder column for a read of no columns (count) */
  private lazy val schemaOrPlaceholder: StructType =
    if (schema.isEmpty) SparkSchemaUtil.getCloudberryPlaceholderSchema(optionsFactory) else schema
  private lazy val createColumnsClause: String =
    SparkSchemaUtil.getCloudberryTableColumns(schemaOrPlaceholder, CbTableTypes.ExternalWritable)
  /**
   * tender: the distribution of the source if the read has all its columns, else randomly (the rows stay on
   * their segments): before, a read without the distribution key (.select("note"), a pushed filter on the key)
   * failed — "column named in DISTRIBUTED BY clause does not exist"; replicated — not allowed for external tables
   */
  private lazy val externalDistributedBy: String = {
    val keys = distributionColNames.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).filter(_.nonEmpty)
    if (schema.isEmpty || distributedByClause.isEmpty) ""
    else if (keys.nonEmpty && keys.forall(k => schema.fieldNames.exists(_.equalsIgnoreCase(k)))) distributedByClause
    else "DISTRIBUTED RANDOMLY"
  }
  private lazy val colListInsert: String = SparkSchemaUtil.getCloudberryTableColumns(schemaOrPlaceholder, CbTableTypes.None)
  private lazy val colListSelect: String =
    if (schema.nonEmpty) SparkSchemaUtil.getCloudberrySelectColumns(schema, CbTableTypes.Target, sourceMeta) else "null"

  /** The rows Cloudberry has sent for the row count check, None — a custom sqltransfer (not comparable) */
  private def runEpoch(transfer: TransferBatch): Option[Long] = {
    if (splitActive)
      return runPart(transfer.parts.head, transfer)
    val (startJson, endJson) = Option(epochOffsets.get(transfer.epoch)).getOrElse((startOffset.json(), endOffset.json()))
    def insertSql(externalTableName: String): String = {
      var insertSelectSql = ""
      if (optionsFactory.sqlTransfer.isEmpty) {
        insertSelectSql = s"insert into $externalTableName (${colListInsert}) "
        if (dbTableName.nonEmpty) {
          insertSelectSql += s"select $colListSelect from $dbTableName"
        } else {
          insertSelectSql += s"select $colListSelect from ($tableOrQuery) sqr"
        }
        if (whereClause.nonEmpty)
          insertSelectSql += s" where $whereClause"
      } else {
        insertSelectSql = optionsFactory.sqlTransfer.replaceAll("(?i)<ext_table>", externalTableName).
          replaceAll("(?i)<select_colList>", colListSelect).
          replaceAll("(?i)<current_epoch>", transfer.epoch.toString)
        if (whereClause.nonEmpty)
          insertSelectSql = insertSelectSql.replaceAll("(?i)<pushed_filters>", whereClause)
      }
      if (connectorMode != CBConnectorModes.Batch) {
        insertSelectSql = insertSelectSql.
          replaceAll("(?i)<start_offset_json>", startJson).
          replaceAll("(?i)<end_offset_json>", endJson)
      }
      insertSelectSql
    }
    withConnection { conn =>
      try {
        val nRows = runTransfer(conn, transfer, insertSql)
        if (connectorMode == CBConnectorModes.Batch && !conn.getAutoCommit)
          conn.commit()
        if (optionsFactory.sqlTransfer.isEmpty) Some(nRows.toLong) else None
      } finally {
        if (connectorMode == CBConnectorModes.Batch) { // back to the pool between executions of the scan
          conn.close()
          dbConnection = null
        }
      }
    }
  }

  /**
   * tender: a part of a split read — its leaf partitions into the one location of its task, on a connection
   * of its own (the parts run at once: neither the scan connection nor the pool — its size would limit
   * them), in a transaction of its own
   */
  private def runPart(part: Int, transfer: TransferBatch): Option[Long] = {
    // the run of a part is <execution>/<partition>.<attempt>: the execution shares a snapshot
    val execution = transfer.run.substring(0, transfer.run.lastIndexOf('/'))
    val snapshot = if (optionsFactory.readSplitSnapshot) splitSnapshots.acquire(execution) else None
    var done = false
    val conn = CBClient.getConn(optionsFactory)
    try {
      snapshot match {
        case Some(id) =>
          conn.setAutoCommit(false)
          conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
          CBClient.executeStatement(conn, s"set transaction snapshot '$id'") // the first statement of the transaction
        case None =>
          conn.setAutoCommit(true)
      }
      val p = splitParts(part)
      val conditions = Seq(p.condition, whereClause.trim).filter(_.nonEmpty)
      val where = if (conditions.isEmpty) "" else conditions.map(c => s"($c)").mkString(" where ", " and ", "")
      val nRows = runTransfer(conn, transfer, externalTableName => s"insert into $externalTableName ($colListInsert) " +
        p.sources.map(source => s"select $colListSelect from $source$where").mkString(" union all "))
      if (!conn.getAutoCommit)
        conn.commit()
      done = true
      Some(nRows.toLong)
    } finally {
      try conn.close() finally snapshot.foreach(_ => splitSnapshots.release(execution, part, done))
    }
  }

  /** tender: the snapshots of the executions of a split read; released after a minute without a part running */
  private lazy val splitSnapshots = new SplitSnapshots(optionsFactory, splitParts.size, 60000L)

  /**
   * One transfer: a writable external table with the gpfdist locations of the tasks of the batch, the
   * INSERT into it (insertSql of the table name), the table dropped after. The INSERT is cancellable: the
   * session cancels it when all readers of the batch stop early (a limit) or its run aborts.
   * @return the rows of the INSERT
   */
  private def runTransfer(conn: Connection, transfer: TransferBatch, insertSql: String => String): Int = {
    val externalTableName = s"ext${queryId}_${runNo.incrementAndGet()}"
    val locationClause = transfer.urls.map(url => s"'$url'").mkString(", ")
    val createExternalTable = s"CREATE WRITABLE EXTERNAL ${tempTableClause} TABLE $externalTableName" +
      s"($createColumnsClause)" +
      s" LOCATION (${locationClause})" +
      // tender: the default NULL \N (before: NULL 'NULL' — an empty string and "NULL" were read as NULL)
      s" FORMAT 'TEXT'" +
      s" ENCODING 'UTF8'" +
      s" $externalDistributedBy"
    val insertSelectSql = insertSql(externalTableName)
    logTrace(s"createExternalTable: $createExternalTable\nSQL: $insertSelectSql")
    try {
      if (!useTempExtTables)
        CBClient.executeStatement(conn, s"DROP EXTERNAL TABLE IF EXISTS $externalTableName")
      CBClient.executeStatement(conn, createExternalTable)
      val nRows = CBClient.executeStatement(conn, insertSelectSql, optionsFactory.dbMessageLogLevel,
        statement => transfer.onCancel(() => statement.cancel()))
      logDebug(s"Scan $queryId run ${transfer.run}: ${transfer.urls.size} tasks, '$insertSelectSql' nRows=$nRows")
      nRows
    } finally {
      // a TEMP table too: sessions of the shared pool outlive the scan
      try {
        CBClient.executeStatement(conn, s"DROP EXTERNAL TABLE IF EXISTS $externalTableName")
      } catch {
        case e: Exception => logDebug(s"DROP EXTERNAL TABLE $externalTableName: ${e.getMessage}")
      }
    }
  }

  // ---------- micro-batch offsets ----------

  private var lastOffsetGetTime: Long = 0

  def getEndOffset: Offset = {
    // tender: re-read on every poll (at most every 100 ms) — Spark calls latestOffset once per trigger
    if (System.currentTimeMillis() - lastOffsetGetTime > 100) {
      withConnection(conn => endOffset.extract(conn, optionsFactory.offsetRestoreSql))
      lastOffsetGetTime = System.currentTimeMillis()
    }
    CBOffset(endOffset.get())
  }

  def commit(end: Offset): Unit = {
    val offset = end match {
      case offset: CBOffset =>
        logDebug(s"Commit: endOffset ${offset.get()}")
        offset
      case _ => null
    }
    dbConnectionGuard.synchronized {
      if (offset != null && optionsFactory.offsetStoreSql.nonEmpty)
        offset.store(cbConnect(), optionsFactory.offsetStoreSql)
      if (dbConnection != null) {
        // stream.read.autocommit=false: the transfer of the epoch and offset.update commit together here
        if (!dbConnection.getAutoCommit)
          dbConnection.commit()
        dbConnection.close()
        dbConnection = null
      }
    }
  }

  def stop(): Unit = {
    session.close("scan stopped")
    if (splitParts.nonEmpty)
      splitSnapshots.close()
    try {
      dbConnectionGuard.synchronized {
        batch = null
        microBatch = null
        if (dbConnection != null) {
          if (connectorMode == CBConnectorModes.Batch) {
            logInfo(s"Batch reader stop")
          } else {
            logInfo(s"Streaming reader stop: endOffset=${endOffset.get()}")
          }
          if (!dbConnection.getAutoCommit)
            dbConnection.rollback() // an epoch without commit(end) is read again after restart
          dbConnection.close()
          dbConnection = null
          rowSet.clear()
        }
      }
    } catch {
      case e: Throwable => logWarning(s"Scan $queryId stop: $e")
    }
  }
}

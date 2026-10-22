package org.apache.spark.sql.tender.cloudberry.connector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.util.Utils

/**
 *
 * @param params Map[String, String] - Note: in Spark 2.x.x DataSourceOptions option names
 *     are implicitly converted to the lower case
 */
case class CBOptionsFactory(params: Map[String, String])
  extends Logging
{
  /**
   * tender: the options by name in any case, as Spark data source options are: before, the names with capitals
   * (sqlTransfer, pool.maxSize, transfer.settleMs, ...) were taken only as written — sqltransfer, as the
   * documentation had it, was ignored silently
   */
  private val options: CaseInsensitiveMap[String] = CaseInsensitiveMap(params)
  if (!options.contains("url")) throw new java.lang.IllegalArgumentException("Must specify Cloudberry DB url option")
  if (!options.contains("dbtable") && !options.contains("sqlTransfer"))
    throw new java.lang.IllegalArgumentException("Must specify dbtable or sqlTransfer option")
  val gpfdistTimeout: Long = Utils.timeStringAsMs(options.getOrElse("server.timeout", "-1"))
  val tableOrQuery: String = options.getOrElse("dbtable", "").trim
  val driver: String = "org.postgresql.Driver"
  val dbTimezone: String = options.getOrElse("dbtimezone", java.time.ZoneId.systemDefault.toString).trim
  val url: String = options("url")
  val user: String = options.getOrElse("user", null)
  val password: String = options.getOrElse("password", null)
  /**
   * tender: gpfdist port(s) of the tasks — every task has its own server now: 0 — ephemeral, N — that port
   * (one task per host at a time), A-B — the first free one of the range (for a firewall)
   */
  val serverPortRange: (Int, Int) = options.getOrElse("server.port", "0").trim.split("-", 2) match {
    case Array(single) => (single.trim.toInt, single.trim.toInt)
    case Array(from, to) => (from.trim.toInt, to.trim.toInt)
  }
  /**
   * tender: server.compress=true — a read asks the Cloudberry segments for zstd: they send the rows compressed
   * (the segments of Cloudberry offer it, the X-GP-ZSTD protocol of gpfdist --compress), the task unpacks them —
   * less network, more CPU on the segments; false (default) — plain text. A write is not compressed.
   */
  val serverCompress: Boolean = options.getOrElse("server.compress", "false").trim.toBoolean
  /** tender: port of the transfer coordinator on the driver (one per driver JVM, the first one wins), 0 — ephemeral */
  val coordinatorPort: Int = options.getOrElse("coordinator.port", "0").trim.toInt
  /**
   * tender: write tasks registered while no new one comes for that long form a batch (an INSERT) — waves
   * when not all of them run at once; all of them at once otherwise
   */
  val transferSettleMs: Long = options.getOrElse("transfer.settleMs", "1000").trim.toLong
  val truncate: Boolean = options.getOrElse("truncate", "false").toBoolean
  val distributedBy: String = options.getOrElse("distributedby", "").trim
  val partitionClause: String = options.getOrElse("partitionclause","").trim
  /**
   * tender: the storage of a table a write creates, before its DISTRIBUTED BY: `using pax`, `with
   * (appendoptimized=true, orientation=column, compresstype=zstd)`; empty — the default of the server (heap)
   */
  val storageClause: String = options.getOrElse("storageclause", "").trim
  val sqlTransfer: String = options.getOrElse("sqlTransfer", "").trim
  val dbSchema: String = options.getOrElse("dbschema", null)
  val bufferSize: Int = options.getOrElse("buffer.size", "1000000").toInt
//  val heartbeatInterval: Long = Utils.timeStringAsMs(params.getOrElse("spark.executor.heartbeatinterval", "10s"))
//  val fetchTimeout: Long = Utils.timeStringAsMs(params.getOrElse("spark.files.fetchtimeout", "60s"))
  val networkTimeout: Long = Utils.timeStringAsMs(options.getOrElse("network.timeout",
    s"${Utils.timeStringAsMs(options.getOrElse("spark.network.timeout", "120s"))/2}ms"
  ))
  /**
   * tender: limit of queued transfer bytes per task buffer (backpressure), e.g. 64m, 256m; 0 = unbounded
   * (the old behaviour: a fast side piles the whole transfer up in the heap)
   */
  val bufferMax: Long = Utils.byteStringAsBytes(options.getOrElse("buffer.max", "64m").trim)
  /** tender: JDBC pool of the driver, one per connection settings in the JVM (CBClient) */
  val poolMaxSize: Int = options.getOrElse("pool.maxSize", "8").trim.toInt
  val poolMinIdle: Int = options.getOrElse("pool.minIdle", "0").trim.toInt
  val poolTimeoutMs: Long = options.get("pool.timeoutMs").map(_.trim.toLong).getOrElse(networkTimeout)
  /** tender: idle pooled connections are closed after that (the eviction runs every half of it) */
  val poolIdleMs: Long = options.getOrElse("pool.idleMs", "60000").trim.toLong
  val dbMessageLogLevel: String = options.getOrElse("dbmessages", "OFF").trim
  val useTempExtTables: Boolean = options.getOrElse("tempexttables", "true").toBoolean
  val offsetStoreSql: String = options.getOrElse("offset.update", "").trim
  val offsetRestoreSql: String = options.getOrElse("offset.select", "").trim
  val undoSideEffectsSQL: String = options.getOrElse("undo.side.effects.sql", "").trim
  val readStreamAutoCommit: Boolean = options.getOrElse("stream.read.autocommit", "true").toBoolean
  val saveMode: SaveMode = options.getOrElse("mode", "append") match {
    case "append" => SaveMode.Append
    case "overwrite" => SaveMode.Overwrite
    case "error" | "errorifexists" => SaveMode.ErrorIfExists
    case "ignore" => SaveMode.Ignore
    case _ => SaveMode.Append
  }
  val actionName: String = options.getOrElse("action.name", tableOrQuery).trim
  /**
   * tender: parallel tasks (gpfdist instances) — for read at most that many partitions, for write that many
   * write tasks (the input is repartitioned); 0 (default) = auto. Both are capped by the primary segments
   * and default_max_read_segments / default_max_write_segments.
   */
  val parallelism: Int = intOption("parallelism", 0, 0)

  /** tender: at most that many Cloudberry segments (gpfdist locations, read partitions) for a read */
  val maxReadSegments: Int = intOption("default_max_read_segments", 64, 1)

  /** tender: at most that many Cloudberry segments (gpfdist locations, write tasks) for a write */
  val maxWriteSegments: Int = intOption("default_max_write_segments", 64, 1)

  /**
   * tender: read.split — a batch read in independent parts, every Spark partition is its own transfer of a
   * part, so a failed task reads only its part again: partitions — groups of the leaf partitions of a
   * partitioned table; segments — the Cloudberry segments (gp_segment_id); column — ranges of
   * read.split.column (as partitionColumn of Spark JDBC); none (default) — one transfer for all the read
   * tasks of a run
   */
  val readSplit: String = options.getOrElse("read.split", "none").trim.toLowerCase(java.util.Locale.ROOT) match {
    case v @ ("none" | "partitions" | "segments" | "column") => v
    case other => throw new java.lang.IllegalArgumentException(
      s"read.split must be none, partitions, segments or column, got: $other")
  }

  /**
   * tender: parts of a split read, 0 (default): partitions, column — 4 x task slots (at most the leaf
   * partitions / the values of the range), segments — a part per primary segment
   */
  val readSplitParts: Int = intOption("read.split.parts", 0, 0)

  /** tender: read.split=column — the column (integral, date, timestamp) and the bounds of its range (strides) */
  val readSplitColumn: String = options.getOrElse("read.split.column", "").trim
  val readSplitLowerBound: String = options.getOrElse("read.split.lowerBound", "").trim
  val readSplitUpperBound: String = options.getOrElse("read.split.upperBound", "").trim

  /**
   * tender: the parts of one execution of a split read see one snapshot (pg_export_snapshot on the driver,
   * SET TRANSACTION SNAPSHOT in every part); false — every part reads its own (current) data
   */
  val readSplitSnapshot: Boolean = options.getOrElse("read.split.snapshot", "true").trim.toBoolean

  /**
   * tender: write.repartition=false — the input is written as it is partitioned, a write task per input
   * partition (more than primary segments — INSERT waves in the one transaction); true (default) — Spark
   * repartitions it by the hash of the columns into the write parallelism (a shuffle of the whole input)
   */
  val writeRepartition: Boolean = options.getOrElse("write.repartition", "true").trim.toBoolean

  /**
   * tender: write.staging=true — a batch write through staging tables: every task attempt fills one of its own,
   * the job commit moves them into the target in one transaction; a failed task writes only its part again;
   * false (default) — all the write tasks INSERT into the target in one transaction
   */
  val writeStaging: Boolean = options.getOrElse("write.staging", "false").trim.toBoolean

  /**
   * tender: write.direct=true — a write of a DataFrame that is a plain read of Cloudberry goes between the clusters
   * without the Spark tasks (DirectTransfer): one INSERT ... SELECT in one session, gpfdist relays on the driver
   * between two; any other DataFrame — through the Spark tasks
   */
  val writeDirect: Boolean = options.getOrElse("write.direct", "false").trim.toBoolean

  private def intOption(name: String, default: Int, min: Int): Int = {
    val value = options.getOrElse(name, default.toString).trim
    val n = scala.util.Try(value.toInt).getOrElse(min - 1)
    if (n < min) throw new java.lang.IllegalArgumentException(s"$name must be an integer >= $min, got: $value")
    n
  }

  /** tender: n limited by the parallelism option (when it is set) */
  def limitParallelism(n: Int): Int = if (parallelism > 0) math.min(n, parallelism) else n

  /**
   * Dump passed in parameters for debug purposes
   * @return multiline String with key -> value pairs
   */
  def dumpParams(): String = {
    val ret: StringBuilder = new StringBuilder(1000)
    val excludeParams = Map("password"-> "")
    (params -- excludeParams.keySet).foreach(p => ret ++= s""""${p._1}" -> "${p._2}"\n""")
    if (ret.nonEmpty)
      ret.setLength(ret.length - 1)
    ret.toString()
  }

  def getJDBCOptions(tableName: String = tableOrQuery): JDBCOptions = {
    val excludeParams = Map(
      "driver" -> "",
      "dbtable" -> "",
      "partitions" -> "",
      //"dbschema" -> "",
      "partitionColumn" -> "",
      "dbTimezone" -> "",
      "truncate" -> "",
      "distributedBy" -> "",
      "storageclause" -> "", // tender
      "iteratorOptimization" -> "",
      "server.path" -> "",
      "server.port" -> "",
      "server.useHostname" -> "",
      "server.hostEnv" -> "",
      "server.nic" -> "",
      "server.timeout" -> "",
      "server.compress" -> "", // tender
      "pool.maxSize" -> "",
      "pool.timeoutMs" -> "",
      "pool.minIdle" -> "",
      "pool.idleMs" -> "", // tender
      "buffer.size"-> "",
      "buffer.max" -> "", // tender
      "network.timeout" -> "",
      "spark.network.timeout" -> "",
      "spark.files.fetchTimeout" -> "",
      "spark.executor.heartbeatInterval" -> "",
      "sqlTransfer" -> "",
      "dbmessages" -> "",
      "tempexttables" -> "",
      "offset.update" -> "",
      "offset.select" -> "",
      "undo.side.effects.sql" -> "",
      "applicationname" -> "", // Spark-2 case workaround
      "ApplicationName" -> "",
      "mode" -> "",
      "action.name" -> "",
      "parallelism" -> "", // tender
      "default_max_read_segments" -> "", // tender
      "default_max_write_segments" -> "", // tender
      "read.split" -> "", // tender
      "read.split.parts" -> "", // tender
      "read.split.column" -> "", // tender
      "read.split.lowerBound" -> "", // tender
      "read.split.upperBound" -> "", // tender
      "read.split.snapshot" -> "", // tender
      "write.repartition" -> "", // tender
      "write.staging" -> "", // tender
      "write.direct" -> "", // tender
      "coordinator.port" -> "", // tender
      "transfer.settleMs" -> "" // tender
    )
    // tender: excluded by name in any case (the keys of the scan options come lowercased)
    val excluded: Set[String] = excludeParams.keySet.map(_.toLowerCase(java.util.Locale.ROOT))
    val passed: Map[String, String] = params.filter { case (k, v) => v != null && !excluded.contains(k.toLowerCase(java.util.Locale.ROOT)) }
    var amendParams: Map[String, String] = passed ++ Map("driver" -> driver,
          "dbtable" -> {if (tableName.nonEmpty) tableName else "-"})
    //import org.postgresql.PGProperty
    import org.apache.spark.SparkContext
    val appId: String = try {
      options.getOrElse("applicationname", SparkContext.getOrCreate.applicationId)
    } catch {
      // SparkContext can raise "SparkException: A master URL must be set in your configuration" when called in executor
      case x: Exception => ""
      //case x: SparkException => ""
    }
    if (appId != "")
      amendParams = amendParams ++ Map("ApplicationName" -> appId)
    // tender: the connection of a transfer INSERT has no traffic for as long as the INSERT runs (hours for
    // a large table) — a firewall / NAT may drop it as idle; keepalive probes start after the OS keepalive
    // time. The tcpKeepAlive option or the URL parameter (wins over properties in the driver) turns it off
    if (!options.contains("tcpKeepAlive"))
      amendParams = amendParams ++ Map("tcpKeepAlive" -> "true")
    logDebug(s"${amendParams.mkString("\n{", "\n", "}")}")
    new JDBCOptions(CaseInsensitiveMap(amendParams))
  }
}

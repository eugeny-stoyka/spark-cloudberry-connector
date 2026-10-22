package org.apache.spark.sql.tender.cloudberry.connector

import com.tender.cloudberry.connector.CBClient
import com.tender.cloudberry.connector.transfer.GpfdistRelay
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, Project, RepartitionOperation, Sort}
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, LeafV2CommandExec}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

import java.sql.Connection
import java.util.UUID

/**
 * tender: write.direct — the source of a transfer without the Spark tasks: a read of Cloudberry (CloudberryScan)
 * with the columns and the filters Spark pushed into it. `select` gives its columns as the transfer of a read does
 * (named as the fields of `schema`); `stop` releases the scan.
 */
case class DirectSource(options: CBOptionsFactory, connection: () => Connection, schema: StructType, select: String,
                        stop: () => Unit)

/** tender: write.direct — the target of a write prepared by the write builder (created / truncated) */
case class DirectTarget(options: CBOptionsFactory, connection: () => Connection, schema: StructType,
                        meta: Map[String, CBColumnMeta], table: String, ignoreInsert: Boolean, truncateFirst: Boolean)

trait DirectTransferSource {
  /** Left — why the read cannot be a source of a direct transfer */
  def directSource: Either[String, DirectSource]
}

trait DirectTransferTarget {
  def directTarget: DirectTarget
}

/**
 * tender: write.direct=true — a write of a DataFrame that is a plain read of Cloudberry (its columns, renamed or
 * not, the filters Spark pushed into the read) goes between the clusters without the Spark tasks. A strategy of
 * the Spark planner (in front of the built-in ones: SparkSession.experimental) sees the plan of the write and
 * replaces it by CloudberryDirectTransfer (see `explain()`):
 *   - one session (the same url, user, password, dbschema): one INSERT ... SELECT inside Cloudberry;
 *   - else (another cluster, other credentials): gpfdist relays on the driver ([[GpfdistRelay]]) — the segments of
 *     the source push the rows, the segments of the target read them, each side under its own credentials.
 * The values are the ones of the transfer through Spark: the external tables of a read and of a write and their
 * TEXT format, without the parsing and the formatting in Spark. Any other DataFrame (computed columns, joins,
 * filters Spark keeps, a cache) is written through the Spark tasks as without the option, with an INFO line why.
 */
object DirectTransfer extends SparkStrategy with Logging {

  /** the strategy into the planner of the session, once (CloudberryDataSource.getTable) */
  def install(session: SparkSession): Unit = session.synchronized {
    val experimental = session.experimental
    if (!experimental.extraStrategies.contains(this))
      experimental.extraStrategies = experimental.extraStrategies :+ this
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case a: AppendData => planned(a.write, a.query)
    case o: OverwriteByExpression => planned(o.write, o.query)
    case _ => Nil
  }

  private def planned(write: Option[Write], query: LogicalPlan): Seq[SparkPlan] = write match {
    case Some(w: DirectTransferTarget) if w.directTarget.options.writeDirect =>
      val target = w.directTarget
      val source =
        if (target.options.sqlTransfer.nonEmpty) Left("the write has its own sqltransfer")
        else if (target.table.isEmpty) Left("no target table")
        else sourceOf(query, target.schema)
      source match {
        case Right((s, columns)) => Seq(DirectTransferExec(s, columns, target))
        case Left(reason) =>
          logInfo(s"write.direct is not applied to ${target.table}: $reason; the rows go through the Spark tasks")
          Nil
      }
    case _ => Nil
  }

  /** the read the DataFrame is and, for every column of the write, the column of the read; Left — why not */
  private def sourceOf(query: LogicalPlan, targetSchema: StructType): Either[String, (DirectSource, Seq[Int])] = {
    // the repartition (and an ordering) Spark adds for the write do not change the rows of a table
    def strip(p: LogicalPlan): LogicalPlan = p match {
      case r: RepartitionOperation => strip(r.child)
      case s: Sort => strip(s.child)
      case other => other
    }
    val (projection, scanPlan) = strip(query) match {
      case Project(list, child) => (Some(list), strip(child))
      case other => (None, other)
    }
    scanPlan match {
      case relation: DataSourceV2ScanRelation => relation.scan match {
        case scan: DirectTransferSource => scan.directSource match {
          case Left(reason) => Left(reason)
          case Right(source) =>
            val ids = relation.output.map(_.exprId)
            val columns = projection.getOrElse(relation.output).map {
              case a: AttributeReference => ids.indexOf(a.exprId)
              case Alias(a: AttributeReference, _) => ids.indexOf(a.exprId)
              case _ => -1
            }
            if (columns.contains(-1))
              Left("the DataFrame computes columns in Spark")
            else if (columns.size != targetSchema.size ||
              columns.zip(targetSchema.fields).exists { case (i, f) => source.schema(i).dataType != f.dataType })
              Left("the columns of the DataFrame differ from the ones of the write")
            else
              Right((source, columns))
        }
        case other => Left(s"the DataFrame does not read Cloudberry (${other.getClass.getSimpleName})")
      }
      case other => Left(s"the DataFrame is not a plain read of Cloudberry: ${other.nodeName} in Spark")
    }
  }

  /** one session: the same cluster and credentials — the target may read the source itself */
  def oneSession(source: DirectSource, target: DirectTarget): Boolean =
    source.options.url == target.options.url && source.options.user == target.options.user &&
      source.options.password == target.options.password && source.options.dbSchema == target.options.dbSchema

  /**
   * the column of a value in the writable external table of a read and the value as the transfer through Spark has
   * it: Spark writes a boolean as 1 / 0 (Cloudberry — as t / f, which a bit column of a target does not take)
   */
  private def transferColumn(field: StructField, value: String): (String, String) = field.dataType match {
    case BooleanType => ("TEXT", s"(case when $value then '1' when not $value then '0' end)")
    case _ => (columnType(field, CbTableTypes.ExternalWritable, null), value)
  }

  /** the type of a column in an external table of the transfer ("name TYPE" of getCloudberryTableColumns) */
  private def columnType(field: StructField, kind: CbTableTypes.CbTableType, meta: Map[String, CBColumnMeta]): String =
    SparkSchemaUtil.getCloudberryTableColumns(StructType(Seq(field)), kind, meta).trim.stripPrefix(field.name).trim

  private[connector] def execute(source: DirectSource, columns: Seq[Int], target: DirectTarget): Unit = {
    if (target.ignoreInsert)
      return
    if (target.options.writeStaging)
      logInfo(s"write.staging is not used by write.direct into ${target.table}: the write is one transaction")
    val start = System.currentTimeMillis()
    val sourceFields = columns.map(source.schema(_))
    val insertColumns = SparkSchemaUtil.getCloudberryTableColumns(target.schema, CbTableTypes.None)
    val readableSelect = SparkSchemaUtil.getCloudberrySelectColumns(target.schema, CbTableTypes.ExternalReadable, target.meta)
    val conn = target.connection()
    try {
      conn.setAutoCommit(false)
      if (target.truncateFirst)
        CBClient.executeStatement(conn, s"truncate table ${target.table}")
      val (rows, how) = if (oneSession(source, target)) {
        // every value as the transfer through Spark gives it: of the type of the column of the writable external
        // table of a read, then through its text into the type of the readable one of a write
        val values = sourceFields.indices.map { j =>
          val (writable, transferred) = transferColumn(sourceFields(j), s"s.${sourceFields(j).name}")
          val readable = columnType(target.schema(j), CbTableTypes.ExternalReadable, target.meta)
          val value = s"cast($transferred as $writable)"
          (if (writable.equalsIgnoreCase(readable)) value else s"cast(cast($value as text) as $readable)") +
            s" as ${target.schema(j).name}"
        }
        val st = conn.createStatement()
        val n = try st.executeLargeUpdate(s"insert into ${target.table} ($insertColumns) select $readableSelect " +
          s"from (select ${values.mkString(", ")} from (${source.select}) s) t") finally st.close()
        (n, "INSERT ... SELECT inside Cloudberry")
      } else {
        val sourceConn = source.connection()
        try {
          sourceConn.setAutoCommit(true)
          val relays = Seq(CBClient.queryNSegments(sourceConn), CBClient.queryNSegments(conn),
            source.options.maxReadSegments, target.options.maxWriteSegments) ++
            Seq(target.options.parallelism).filter(_ > 0)
          val ext = s"ext_direct_${UUID.randomUUID().toString.replace("-", "")}"
          val temp = if (target.options.useTempExtTables) "temp" else ""
          def locations(urls: Seq[String]) = urls.map(u => s"'$u'").mkString(", ")
          val sourceSide = GpfdistRelay.Side(
            urls => s"create writable external $temp table ${ext}_w " +
              s"(${target.schema.fields.zip(sourceFields).map { case (t, f) => s"${t.name} ${transferColumn(f, "")._1}" }.mkString(", ")}) " +
              s"location (${locations(urls)}) format 'TEXT' encoding 'UTF8' distributed randomly",
            _ => s"insert into ${ext}_w select ${sourceFields.map(f => transferColumn(f, s"s.${f.name}")._2).mkString(", ")} " +
              s"from (${source.select}) s",
            s"drop external table if exists ${ext}_w")
          val targetSide = GpfdistRelay.Side(
            urls => s"create readable external $temp table ${ext}_r " +
              s"(${SparkSchemaUtil.getCloudberryTableColumns(target.schema, CbTableTypes.ExternalReadable, target.meta)}) " +
              s"location (${locations(urls)}) format 'TEXT' (newline 'LF') encoding 'UTF8'",
            _ => s"insert into ${target.table} ($insertColumns) select $readableSelect from ${ext}_r",
            s"drop external table if exists ${ext}_r")
          val n = GpfdistRelay.run(math.max(1, relays.min), target.options.serverPortRange, target.options.bufferSize,
            target.options.bufferMax, sourceConn, sourceSide, conn, targetSide)
          (n, s"gpfdist relays on the driver: ${math.max(1, relays.min)} streams")
        } finally sourceConn.close()
      }
      conn.commit()
      logInfo(s"write.direct into ${target.table}: $rows rows, ${System.currentTimeMillis() - start} ms, $how")
    } finally {
      conn.close() // back to the pool, rolled back there if not committed
    }
  }
}

/** tender: the plan of a write.direct transfer — on the driver, no Spark task */
case class DirectTransferExec(source: DirectSource, columns: Seq[Int], target: DirectTarget) extends LeafV2CommandExec {
  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    try DirectTransfer.execute(source, columns, target) finally source.stop()
    Nil
  }

  override def simpleString(maxFields: Int): String =
    s"CloudberryDirectTransfer into ${target.table}: " +
      (if (DirectTransfer.oneSession(source, target)) "INSERT ... SELECT inside Cloudberry" else "gpfdist relays on the driver")
}

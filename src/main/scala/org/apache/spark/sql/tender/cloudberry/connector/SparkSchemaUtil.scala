package org.apache.spark.sql.tender.cloudberry.connector

import java.io.StringWriter
import java.sql.{Connection, Date, JDBCType, ResultSetMetaData, SQLException, Timestamp}
import java.text.Format
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.temporal._
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.tender.cloudberry.connector.CBClient
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import SparkSchemaUtil.rightPadWithChar
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column}
import org.apache.spark.sql.connector.expressions.{FieldReference, Expression => V2Expression}
import org.apache.spark.sql.tender.cloudberry.connector.CbTableTypes.CbTableType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap => MutableHashMap}

//import org.apache.commons.text.StringUtils.rightPad
import SparkSchemaUtil.{escapeKey, escapeText, cbScalarType, isClass, isCbArray, isJsonType, unescapeText}

object SparkSchemaUtil {

  val ESCAPE_CHARACTER = "\""
  val NAME_SEPARATOR = "."

  /** tender: the Cloudberry type of a scalar Spark type; None — arrays, maps, structs, binary */
  def cbScalarType(dataType: DataType): Option[String] = dataType match {
    case StringType => Some("TEXT")
    case IntegerType => Some("INTEGER")
    // tender: SMALLINT (was INTEGER) — read back as smallint; Cloudberry has no one-byte integer
    case ShortType | ByteType => Some("SMALLINT")
    case LongType => Some("BIGINT")
    case DoubleType => Some("DOUBLE PRECISION")
    case FloatType => Some("REAL")
    case BooleanType => Some("BOOLEAN")
    case TimestampType | TimestampNTZType => Some("TIMESTAMP") // tender: timestamp_ntz — the local date-time
    case DateType => Some("DATE")
    case t: DecimalType => Some(s"DECIMAL(${t.precision},${t.scale})")
    case _ => None
  }

  /** tender: an array of scalars — a Cloudberry array (INTEGER[], TEXT[], ...), transferred as an array literal */
  def isCbArray(dataType: DataType): Boolean = dataType match {
    case ArrayType(elementType, _) => cbScalarType(elementType).isDefined
    case _ => false
  }

  /** tender: maps, structs, arrays of anything else (arrays, maps, structs, binary) — JSONB, transferred as JSON */
  def isJsonType(dataType: DataType): Boolean = dataType match {
    case _: MapType | _: StructType => true
    case a: ArrayType => !isCbArray(a)
    case _ => false
  }

  /** tender: the NULL of the TEXT format of the external tables (the default \N: an empty string and "NULL" stay values) */
  val TEXT_NULL = "\\N"

  /**
   * tender: a value in the TEXT format of Cloudberry external tables: the backslash, LF, CR and the delimiter
   * escaped, everything else as it is (UTF-8). Before: StringEscapeUtils.escapeJava — non-ASCII characters became
   * \\uXXXX, which the TEXT format does not know: "привет" was stored as "u043Fu0440...".
   */
  def escapeText(s: String, delimiter: Char = '\t'): String = {
    var i = 0
    while (i < s.length && "\\\n\r".indexOf(s.charAt(i)) < 0 && s.charAt(i) != delimiter)
      i += 1
    if (i == s.length)
      return s
    val sb = new java.lang.StringBuilder(s.length + 16).append(s, 0, i)
    while (i < s.length) {
      s.charAt(i) match {
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' if delimiter == '\t' => sb.append("\\t")
        case c if c == delimiter => sb.append('\\').append(c)
        case c => sb.append(c)
      }
      i += 1
    }
    sb.toString
  }

  /** tender: a value of the TEXT format (as Cloudberry writes it): \b \f \n \r \t \v, octal \NNN, hex \xHH, \c -> c */
  def unescapeText(s: String): String = {
    if (s.indexOf('\\') < 0)
      return s
    val sb = new java.lang.StringBuilder(s.length)
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (c != '\\' || i + 1 == s.length) {
        sb.append(c)
        i += 1
      } else {
        val e = s.charAt(i + 1)
        i += 2
        e match {
          case 'b' => sb.append('\b')
          case 'f' => sb.append('\f')
          case 'n' => sb.append('\n')
          case 'r' => sb.append('\r')
          case 't' => sb.append('\t')
          case 'v' => sb.append('\u000b')
          case d if d >= '0' && d <= '7' =>
            var code = d - '0'
            var n = 1
            while (n < 3 && i < s.length && s.charAt(i) >= '0' && s.charAt(i) <= '7') {
              code = code * 8 + (s.charAt(i) - '0')
              i += 1
              n += 1
            }
            sb.append(code.toChar)
          case 'x' if i < s.length && Character.digit(s.charAt(i), 16) >= 0 =>
            var code = Character.digit(s.charAt(i), 16)
            i += 1
            if (i < s.length && Character.digit(s.charAt(i), 16) >= 0) {
              code = code * 16 + Character.digit(s.charAt(i), 16)
              i += 1
            }
            sb.append(code.toChar)
          case other => sb.append(other)
        }
      }
    }
    sb.toString
  }

  /**
   * Extracts Cloudberry table columns metadata as a Map[column name String, {@link CBColumnMeta}]
   * @param connection: Cloudberry database JDBC connection
   * @param dbSchemaName: name of DB schema where target table resides
   * @param tableName: name of the target table
   * @return
   */
  def getCloudberryTableMeta(connection: Connection, dbSchemaName: String, tableName: String): Map[String, CBColumnMeta] = {
    val columnsMeta: MutableHashMap[String, CBColumnMeta] = MutableHashMap()
    val schemaPattern: String = if (dbSchemaName == null || dbSchemaName.isEmpty) null else dbSchemaName
    using(connection.getMetaData.getColumns(null, schemaPattern, tableName, null)) {
      rs => {
        while (rs.next()) {
          val colName = rs.getString("COLUMN_NAME")
          var precision: Integer = rs.getInt("COLUMN_SIZE")
          if (rs.wasNull())
            precision = null
          var scale: Integer = rs.getInt("DECIMAL_DIGITS")
          if (rs.wasNull())
            scale = null
          val colMeta = CBColumnMeta(rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME"),
            rs.getInt("NULLABLE"), precision, scale)
          columnsMeta.put(colName, colMeta)
        }
      }
    }
    columnsMeta.toMap
  }

  /**
   * Generates column list claus for
   * <p>   CREATE TABLE SQL operator - when <b>forCreateTable</b> is one of (Target, ExternalReadable, ExternalWritable),
   * <p>   or for
   * <p>   INSERT SQL operator - when <b>forCreateTable</b> is None
   * <p> In the case of CREATE TABLE operation, result contains corresponding column data type specifiers derived from
   * the spark data source schema given by the <b>schema</b> parameter. An optional <b>dbTableMeta</b>,
   *  parameter extracted from the Cloudberry database object, if given, can be used to override data type of (some) columns
   *  matching by column name with accordance to DB preferences, thus providing a mean for data type mapping
   *  between Spark and Cloudberry data sources.
   * @param schema: {@link StructType} - Spark data source schema
   * @param forCreateTable: target table type ({@link CbTableType}): .None, ExternalReadable, ExternalWritable
   * @param dbTableMeta: optional Cloudberry source metadata, can contain subset of target columns that require type mapping
   * @return column list string
   */
  def getCloudberryTableColumns(schema: StructType, forCreateTable: CbTableType, dbTableMeta: Map[String, CBColumnMeta] = null
                              ): String = {
    val columns = new StringBuilder("")
    var i: Int = 0
    schema.foreach(f => {
      if (i > 0) columns.append(", ")
      columns.append(f.name)
      if (forCreateTable != CbTableTypes.None) {
        var dt: String =  f.dataType match {
          case StringType => "TEXT"
          case IntegerType => "INTEGER"
          case LongType => "BIGINT"
          case DoubleType => "DOUBLE PRECISION"
          case FloatType => "REAL"
          case ShortType => "SMALLINT" // tender: was INTEGER
          case ByteType => "SMALLINT" // type BYTE doesn't exists
          case BooleanType => "BOOLEAN"
          case BinaryType => {
            forCreateTable match {
              case CbTableTypes.ExternalReadable => "TEXT"
              case CbTableTypes.ExternalWritable => "TEXT"
              case _ => "BYTEA"
            }
          }
          case TimestampType | TimestampNTZType => "TIMESTAMP"
          case DateType => "DATE"
          case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
          // tender: arrays of scalars — Cloudberry arrays, maps / structs / other arrays — JSONB; a read
          // transfers them as text (the writable external table: Cloudberry casts them to text itself)
          case a: ArrayType if isCbArray(a) =>
            if (forCreateTable == CbTableTypes.ExternalWritable) "TEXT" else s"${cbScalarType(a.elementType).get}[]"
          case t if isJsonType(t) => if (forCreateTable == CbTableTypes.ExternalWritable) "TEXT" else "JSONB"
          case t => throw new IllegalArgumentException(s"Unsupported type ${t.catalogString} of the column ${f.name}")
        }
        // External table column types overriding corresponding to the existing target table
        if ((dbTableMeta != null) && dbTableMeta.contains(f.name)
          && ((dt == "TEXT") || (f.dataType == BooleanType) || (f.dataType == BinaryType))) {
          if (forCreateTable == CbTableTypes.ExternalReadable) {
            dbTableMeta.get(f.name) match {
              case Some(colMeta: CBColumnMeta) => {
                colMeta.dbTypeName.toUpperCase match {
                  case "UUID" => dt = "UUID"
                  case "VARCHAR" | "CHARACTER VARYING" | "CHARACTER" | "CHAR" => if (colMeta.colSize != null) dt = s"VARCHAR(${colMeta.colSize})"
                  case "INTEGER" => dt = "INTEGER"
                  case "BIGINT" => dt = "BIGINT"
                  case "DOUBLE PRECISION" => dt = "DOUBLE PRECISION"
                  case "REAL" => dt = "REAL"
                  // case "BYTE" => dt = "BYTE"
                  case "BIT" | "BIT VARYING" | "VARBIT" => {
                    dt = "VARCHAR(256)"
                    /*
                                      if (colMeta.colSize != null) {
                                        dt = s"BIT(${colMeta.colSize})"
                                      } else {
                                        dt = "BIT(1)"
                                      }
                    */
                  }
                  case "BOOLEAN" => dt = "BOOLEAN"
                  case "DECIMAL" => {
                    if ((colMeta.colSize != null) && (colMeta.decimalDigits != null))
                      dt = s"DECIMAL(${colMeta.colSize}.${colMeta.decimalDigits})"
                  }
                  case "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
                    if (colMeta.colSize != null) {
                      dt = s"TIMESTAMPTZ(${colMeta.colSize})"
                    } else {
                      dt = "TIMESTAMPTZ"
                    }
                  }
                  case "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
                    if (colMeta.colSize != null) {
                      dt = s"TIMESTAMP(${colMeta.colSize})"
                    } else {
                      dt = "TIMESTAMP"
                    }
                  }
                  case "TIME WITH TIME ZONE" => {
                    if (colMeta.colSize != null) {
                      dt = s"TIME (${colMeta.colSize}) WITH TIME ZONE"
                    } else {
                      dt = "TIME WITH TIME ZONE"
                    }
                  }
                  case "TIME" | "TIME WITHOUT TIME ZONE" => {
                    if (colMeta.colSize != null) {
                      dt = s"TIME(${colMeta.colSize})"
                    } else {
                      dt = "TIME"
                    }
                  }
                  case "DATE" => dt = "DATE"
                  case "GEOMETRY" => dt = "GEOMETRY"
                  case _ =>
                }
              }
              case None =>
            }
          }
        }
        columns.append(" ")
        columns.append(dt)
      }
      i += 1
    })
    columns.toString()
  }

  /**
   * Generates column list claus for
   * <p>   SELECT SQL operator - where <b>fromTableOfType</b> parameter can be one of (Target, ExternalReadable),
   * <p> with every column from
   * the spark data source schema given by the <b>schema</b> parameter.
   * <p> An optional <b>dbTableMeta</b>,
   * parameter extracted from the Cloudberry database object, if given, can be used to override
   * a data type of (some) columns
   * matching by column name with accordance to DB preferences, thus providing a mean for data type mapping
   * between Spark and Cloudberry data sources.
   *
   * @param schema         : {@link StructType} - Spark data source schema
   * @param fromTableOfType : target table type ({@link CbTableType}), one of: None, ExternalReadable
   * @param dbTableMeta    : optional Cloudberry source metadata, can contain subset of target columns that require type mapping
   * @return column list string
   */
  def getCloudberrySelectColumns(schema: StructType, fromTableOfType: CbTableType, dbTableMeta: Map[String, CBColumnMeta] = null): String = {
    val columns = new StringBuilder("")
    var i: Int = 0
    def targetType(name: String): Option[String] =
      Option(dbTableMeta).flatMap(_.get(name)).map(_.dbTypeName.toLowerCase(java.util.Locale.ROOT))
    def isText(typeName: String): Boolean =
      Set("text", "varchar", "bpchar", "character varying", "character", "char").contains(typeName)
    /** tender: a Cloudberry type name (typname: int4, _uuid, interval) in a cast, quoted unless a plain one */
    def typeRef(typeName: String): String =
      if (typeName.matches("[a-z_][a-z0-9_]*")) typeName else "\"" + typeName.replace("\"", "\"\"") + "\""
    schema.foreach(f => {
      if (i > 0)
        columns.append(", ")
      var dt: String = f.name
      if (fromTableOfType == CbTableTypes.ExternalReadable) {
        f.dataType match {
          // tender: into an existing column of another type — a Cloudberry array into json / jsonb / text,
          // JSON (the JSONB column of the external table) into json / text
          case a: ArrayType if isCbArray(a) => targetType(f.name) match {
            case Some("json") => dt = s"to_json(${f.name})"
            case Some("jsonb") => dt = s"to_jsonb(${f.name})"
            case Some(t) if isText(t) => dt = s"${f.name}::text"
            // tender: into an array of another element type (uuid[], jsonb[], inet[]): Cloudberry casts an array
            // in an INSERT only when its elements cast so, text does not to them
            case Some(t) if t.startsWith("_") => dt = s"${f.name}::${typeRef(t)}"
            case _ =>
          }
          case t if isJsonType(t) => targetType(f.name) match {
            case Some("json") => dt = s"${f.name}::json"
            case Some(t) if isText(t) => dt = s"${f.name}::text"
            // tender: binary arrays go as JSON arrays of base64 — into bytea[] element by element, in order
            case Some("_bytea") if t == ArrayType(BinaryType, true) || t == ArrayType(BinaryType, false) =>
              dt = s"case when ${f.name} is null then null else array(select decode(e, 'base64') " +
                s"from jsonb_array_elements_text(${f.name}) with ordinality as a(e, n) order by n) end"
            case _ =>
          }
          case BinaryType => dt = s"decode(${f.name}, 'base64')"
          case BooleanType => {
            if ((dbTableMeta != null) && dbTableMeta.contains(f.name)) {
              dbTableMeta.get(f.name) match {
                case Some(colMeta) => {
                  if ((colMeta.dbTypeName.toUpperCase == "BIT") && (colMeta.colSize != null))
                    dt = s"${f.name}::bit(${colMeta.colSize})"
                }
                case None =>
              }
            }
          }
          case StringType => {
            if ((dbTableMeta != null) && dbTableMeta.contains(f.name)) {
              dbTableMeta.get(f.name) match {
                case Some(colMeta) => {
                  if ((colMeta.dbTypeName.toUpperCase == "BIT")
                  || (colMeta.dbTypeName.toUpperCase == "BIT VARYING")
                    || (colMeta.dbTypeName.toUpperCase == "VARBIT")) {
                    dt = s"${f.name}::varbit"
/*
                    if ((colMeta.colSize != null) && (colMeta.colSize > 0)) {
                      dt = s"${f.name}::bit(${colMeta.colSize})"
                    } else {
                      dt = s"${f.name}::varbit"
                    }
*/
                  } else if (colMeta.dbTypeName.toUpperCase == "JSON") {
                    dt = s"${f.name}::json"
                  } else if (colMeta.dbTypeName.toUpperCase == "JSONB") {
                    dt = s"${f.name}::jsonb"
                  } else if (!isText(colMeta.dbTypeName.toLowerCase(java.util.Locale.ROOT))) {
                    // tender: interval, money, inet, point, ...: Cloudberry does not cast text to them in an INSERT
                    // ("column is of type interval but expression is of type text"), an explicit cast does
                    dt = s"${f.name}::${typeRef(colMeta.dbTypeName)}"
                  }
                }
                case None =>
              }
            }
          }
          // tender: a timestamp into time (the time of day) / timetz (of the instant, in the session time zone)
          case TimestampType | TimestampNTZType => targetType(f.name) match {
            case Some("time") => dt = s"${f.name}::time"
            case Some("timetz") => dt = s"${f.name}::timestamptz::timetz"
            case _ =>
          }
          case _ =>
        }
      } else if (fromTableOfType == CbTableTypes.Target) {
        f.dataType match {
          case BinaryType => dt = s"encode(${f.name}, 'base64')"
          // tender: Spark JDBC types bit(1) boolean and time / timetz timestamp, Cloudberry casts neither so in an
          // INSERT into the external table ("column is of type boolean but expression is of type bit"):
          // dbTableMeta here — the source columns; time — on 1970-01-01, as Spark JDBC reads it
          case BooleanType if targetType(f.name).contains("bit") => dt = s"${f.name}::int::boolean"
          case TimestampType | TimestampNTZType if targetType(f.name).exists(t => t == "time" || t == "timetz") =>
            dt = s"(date '1970-01-01' + ${f.name})"
          case _ =>
        }
      }
      columns.append(s"$dt as ${f.name}")
      i += 1
    })
    columns.toString()
  }

  /**
   * Produce dummy fake data scheme required for the count of rows operation.
   * @param optionsFactory
   * @return
   */
  def getCloudberryPlaceholderSchema(optionsFactory: CBOptionsFactory): StructType = {
    val fields: Array[StructField] = new Array[StructField](1)
    val dialect: JdbcDialect = JdbcDialects.get(optionsFactory.getJDBCOptions("--").url)
    val metadata = new MetadataBuilder().putLong("scale", 0)
    val columnType =
      dialect.getCatalystType(java.sql.Types.CHAR, "CHAR", 20, metadata).getOrElse(
        getCatalystType(java.sql.Types.CHAR, 20, 0, false))
    fields(0) = StructField("dummy", columnType, true)
    new StructType(fields)
  }

  /**
   * Extracts columns metadata list as {@link StructType} instance (i.e. Spark-native data schema representation)
   * from the Cloudberry data source given by <b>tableOrQuery</b> parameter,
   * <p>which can be a table name or SQL SELECT operator.
   * <p>No actual data result set is generated, the statement for <b>tableOrQuery</b> is only prepared.
   * <p> <b>Note</b>: this method will commit previous transaction (if any) on the <b>conn</b>
   * @param optionsFactory - can pass usual JDBC options influencing the statement prepare stage.
   * @param conn: Cloudberry DB connection
   * @param tableOrQuery: Cloudberry table name or SELECT operator that can produce a result set
   * @param alwaysNullable optional: enforce all columns in the result to be marked as nullable
   * @return {@link StructType} instance with column list metadata
   */
  def getCloudberryTableSchema(optionsFactory: CBOptionsFactory,
                              conn: Connection,
                              tableOrQuery: String,
                              alwaysNullable: Boolean = false): StructType = {
    if (!conn.getAutoCommit)
      conn.commit()
    JdbcUtils.getSchemaOption(conn, optionsFactory.getJDBCOptions(tableOrQuery)) match {
      case Some(schema) if schema.exists(_.dataType == BinaryType) =>
        val meta = getSourceColumnMeta(conn, tableOrQuery)
        StructType(schema.map(f => meta.get(f.name) match {
          case Some(m) if f.dataType == BinaryType && isBitString(m.dbTypeName, m.colSize) => f.copy(dataType = StringType)
          case _ => f
        }))
      case Some(schema) => schema
      case None => {
        val targetType: CBTarget = CBTarget(tableOrQuery)
        var sql: String = tableOrQuery
        if (!targetType.isQuery) {
          if (CBClient.tableExists(conn, tableOrQuery)) sql = s"select * from $tableOrQuery"
          else return new StructType()
        }
        if (!conn.getAutoCommit)
          conn.commit()
        using(conn.prepareStatement(sql)) {
          stmt => {
            val rsmd = stmt.getMetaData
            val colCount = rsmd.getColumnCount
            val fields: Array[StructField] = new Array[StructField](colCount)
            var i: Int = 0
            val dialect: JdbcDialect = JdbcDialects.get(optionsFactory.getJDBCOptions("--").url)
            while (i < colCount) {
              val columnName = rsmd.getColumnLabel(i + 1)
              val dataType = rsmd.getColumnType(i + 1)
              val typeName = rsmd.getColumnTypeName(i + 1)
              val fieldSize = rsmd.getPrecision(i + 1)
              val fieldScale = rsmd.getScale(i + 1)
              val isSigned = rsmd.isSigned(i + 1)
              val nullable = if (alwaysNullable) {
                true
              } else {
                rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
              }
              val metadata = new MetadataBuilder().putLong("scale", fieldScale)
              val columnType =
                if (isBitString(typeName, fieldSize)) StringType
                else dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
                  getCatalystType(dataType, fieldSize, fieldScale, isSigned))
              fields(i) = StructField(columnName, columnType, nullable)
              i += 1
            }
            new StructType(fields)
          }
        }
      }
    }
  }

  /**
   * tender: bit(n), n > 1 — a string ("10101010"), as varbit: Spark JDBC gives it binary, the bytes of its text;
   * bit(1) stays boolean
   */
  private def isBitString(typeName: String, size: Integer): Boolean =
    typeName == "bit" && (size == null || size != 1)

  /**
   * tender: the Cloudberry types of the columns of a table or a query by name (typname: bit, timetz, _uuid; the
   * precision), from the metadata of the prepared statement — nothing is run
   */
  def getSourceColumnMeta(conn: Connection, tableOrQuery: String): Map[String, CBColumnMeta] = {
    val sql = if (CBTarget(tableOrQuery).isQuery) tableOrQuery else s"select * from $tableOrQuery"
    using(conn.prepareStatement(sql)) { stmt =>
      val md = stmt.getMetaData
      (1 to md.getColumnCount).map(i => md.getColumnLabel(i) ->
        CBColumnMeta(md.getColumnType(i), md.getColumnTypeName(i), md.isNullable(i), md.getPrecision(i), md.getScale(i))).toMap
    }
  }

/*
  private def getConn(optionsFactory: CBOptionsFactory): Connection =
    JdbcUtils.createConnectionFactory(optionsFactory.getJDBCOptions("--"))()
*/

  private def using[A, B <: {
    def close(): Unit
  }](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }

  /**
   * Escapes the given argument with {@value #ESCAPE_CHARACTER}
   *
   * @param argument any non null value.
   * @return
   */
  def getEscapedArgument(argument: String): String = {
    if (argument == null)
      new NullPointerException("Argument passed cannot be null")
    ESCAPE_CHARACTER + argument + ESCAPE_CHARACTER
  }

  def getEscapedFullColumnName(fullColumnName: String): String = {
    if (fullColumnName.startsWith(ESCAPE_CHARACTER)) return fullColumnName
    var index = fullColumnName.indexOf(NAME_SEPARATOR)
    if (index < 0) return getEscapedArgument(fullColumnName)
    val columnFamily = fullColumnName.substring(0, index)
    val columnName = fullColumnName.substring(index + 1)
    getEscapedArgument(columnFamily) + NAME_SEPARATOR + getEscapedArgument(columnName)
  }

  def stripChars(s: String, ch: String): String = s filterNot (ch contains _)

  // Helper function to escape column key to work with SQL queries
  private def escapeKey(key: String): String = getEscapedFullColumnName(key)

  private def isClass(obj: Any, className: String) = {
    className.equals(obj.getClass.getName)
  }

  /**
   * Maps a JDBC type of some generic database to a Catalyst type.  This function is called only when
   * the JdbcDialect class corresponding to Postgres database driver returns null.
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
  private def getCatalystType(
                               sqlType: Int,
                               precision: Int,
                               scale: Int,
                               signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.DECIMAL       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.REF_CURSOR    => null
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType
      case java.sql.Types.STRUCT        => StringType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE
      => StringType //null
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      => StringType //null
      case java.sql.Types.TINYINT       => IntegerType
      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
      case _                            =>
        throw new SQLException("Unrecognized SQL type " + sqlType)
      // scalastyle:on
    }

    if (answer == null) {
      throw new SQLException("Unsupported type " + JDBCType.valueOf(sqlType).getName)
    }
    answer
  }

  def repeatChar(c: Char, n: Int): String = c.toString * n

  def rightPadWithChar(str: String, num: Int, c: Char): String = {
    val len = num - str.length
    if (len <= 0)
      return str
    str + repeatChar(c, len)
  }

  /** tender: how long guessMaxParallelTasks waits for executors to register */
  private val executorsWaitMs: Long = 60000

  def guessMaxParallelTasks(): Int = {
    val sparkContext = SparkContext.getOrCreate
    // tender: in local mode tasks run in the driver JVM and getExecutorMemoryStatus holds the driver
    // only, so the loop below never ended (busy spin at 100% CPU): as many tasks as local cores
    if (sparkContext.isLocal)
      return math.max(1, sparkContext.defaultParallelism)
    var guess: Int = -1
    // tender: wait for executors with a pause and not forever — with dynamic allocation and no
    // executors nothing requests them while the driver waits here; then fall back to 1 task
    val deadline = System.currentTimeMillis() + executorsWaitMs
    while ((guess <= 0) && !Thread.currentThread().isInterrupted && System.currentTimeMillis() < deadline) {
      guess = sparkContext.getExecutorMemoryStatus.keys.size - 1
      if (sparkContext.deployMode == "cluster")
        guess -= 1
      if (guess <= 0)
        try Thread.sleep(100)
        catch { case _: InterruptedException => Thread.currentThread().interrupt() }
    }
    math.max(guess, 1)
  }

  /**
   * tender: task slots of the application now — local: the local cores; cluster: registered executors x
   * spark.executor.cores / spark.task.cpus (at least the executors, at least 1)
   */
  def totalTaskSlots(): Int = {
    val sparkContext = SparkContext.getOrCreate
    if (sparkContext.isLocal)
      return math.max(1, sparkContext.defaultParallelism)
    val executors = math.max(1, guessMaxParallelTasks())
    val cores = sparkContext.getConf.getInt("spark.executor.cores", 1)
    val taskCpus = math.max(1, sparkContext.getConf.getInt("spark.task.cpus", 1))
    math.max(executors, executors * cores / taskCpus)
  }

  /**
   * tender: a speculative copy of a transfer task aborts the whole transfer midway (a transfer is not
   * repeated partially, see TransferSession.register), so a read / write refuses to start with
   * spark.speculation on (Spark does not speculate in local mode)
   * @param action "read" or "write", for the message
   */
  def requireNoSpeculation(action: String): Unit = {
    val sparkContext = SparkContext.getOrCreate
    if (!sparkContext.isLocal && sparkContext.getConf.getBoolean("spark.speculation", false))
      throw new UnsupportedOperationException(s"Cloudberry $action is not supported with spark.speculation=true: " +
        s"a speculative copy of a transfer task would abort the transfer midway; set spark.speculation=false")
  }

  /**
   * tender: clustering of the write distribution for the parallelism option — all columns except
   * those holding maps (hash partitioning rejects MapType); single-part references, so a column
   * name with a dot is not taken for a nested field
   */
  def hashClusteringColumns(schema: StructType): Array[V2Expression] = {
    def hasMap(dataType: DataType): Boolean = dataType match {
      case _: MapType => true
      case a: ArrayType => hasMap(a.elementType)
      case s: StructType => s.fields.exists(f => hasMap(f.dataType))
      case _ => false
    }
    schema.fields.filterNot(f => hasMap(f.dataType)).map(f => FieldReference.column(f.name): V2Expression)
  }

  def schema2Columns(schema: StructType): Array[Column] = CatalogV2Util.structTypeToV2Columns(schema)
}

case class SparkSchemaUtil(dbTimeZoneName: String = java.time.ZoneId.systemDefault.toString) {
  val DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSS]"
  val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
  val CB_TIMESTAMP_FORMAT = "yyyy-mm-dd hh24:mi:ss.us"
  val CB_DATE_FORMAT = "yyyy-mm-dd"
  val dbTimeZoneId: ZoneId = ZoneId.of(dbTimeZoneName) // e.g. "Asia/Irkutsk"
  val timestampParseFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_TIME_FORMAT)
  val timestampParseFormatTz: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_TIME_FORMAT).withZone(dbTimeZoneId)
  val dateFormatter: Format = FastDateFormat.getInstance(DEFAULT_DATE_FORMAT)
  val dateParseFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT)

  private def getTimestampString(timestampValue: java.sql.Timestamp): String = {
    "TO_TIMESTAMP('%s', '%s')".format(
      //timeformatter.format(timestampValue),
      timestampParseFormat.format(timestampValue.toLocalDateTime),
      CB_TIMESTAMP_FORMAT)
  }

  private def getDateString(dateValue: java.sql.Date): String = {
    "TO_DATE('%s', '%s')".format(
      dateFormatter.format(dateValue),
      //dateParseFormat.format(dateValue.toLocalDate),
      CB_DATE_FORMAT)
  }

  private def escapeSql(str: String): String = {
    if (str == null) return null
    StringUtils.replace(str, "'", "''")
  }

  // Helper function to escape string values in SQL queries
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"

    case timestampValue: Timestamp => getTimestampString(timestampValue)

    case dateValue: Date => getDateString(dateValue)

    // tender: the java.time values of filters (timestamp_ntz; timestamps and dates with
    // spark.sql.datetime.java8API.enabled) went into the SQL as their toString, unquoted
    case v: LocalDateTime => s"timestamp '${timestampParseFormat.format(v)}'"
    case v: Instant => s"timestamptz '${v}'"
    case v: LocalDate => s"date '${v}'"

    case utf if (isClass(utf, "org.apache.spark.sql.types.UTF8String")) => s"'${escapeSql(utf.toString)}'"
    // Spark 1.5
    case utf if (isClass(utf, "org.apache.spark.unsafe.types.UTF8String")) => s"'${escapeSql(utf.toString)}'"

    // Pass through anything else
    case _ => value
  }

  /**
   * Attempt to create Postgres-accepted WHERE clause from Spark filters,
   *
   * @return tuple representing where clause (derived from supported filters),
   *         array of unsupported filters and array of supported filters
   */
  def pushFilters(filters: Array[Filter]): (String, Array[Filter], Array[Filter]) = {
    if (filters.isEmpty) {
      return ("", Array[Filter](), Array[Filter]())
    }

    val filter = new StringBuilder("")
    val unsupportedFilters = Array[Filter]();
    var i = 0

    filters.foreach(f => {
      // Assume conjunction for multiple filters, unless otherwise specified
      if (i > 0) {
        filter.append(" AND")
      }

      f match {
        // Spark 1.3.1+ supported filters
        case And(leftFilter, rightFilter) => {
          val (whereClause, currUnsupportedFilters, _) = pushFilters(Array(leftFilter, rightFilter))
          if (currUnsupportedFilters.isEmpty)
            filter.append(whereClause)
          else
            unsupportedFilters :+ f
        }
        case Or(leftFilter, rightFilter) => {
          val (whereLeftClause, leftUnsupportedFilters, _) = pushFilters(Array(leftFilter))
          val (whereRightClause, rightUnsupportedFilters, _) = pushFilters(Array(rightFilter))
          if (leftUnsupportedFilters.isEmpty && rightUnsupportedFilters.isEmpty) {
            filter.append(whereLeftClause + " OR " + whereRightClause)
          }
          else {
            unsupportedFilters :+ f
          }
        }
        case Not(aFilter) => {
          val (whereClause, currUnsupportedFilters, _) = pushFilters(Array(aFilter))
          if (currUnsupportedFilters.isEmpty)
            filter.append(" NOT " + whereClause)
          else
            unsupportedFilters :+ f
        }
        case EqualTo(attr, value) => filter.append(s" ${escapeKey(attr)} = ${compileValue(value)}")
        case GreaterThan(attr, value) => filter.append(s" ${escapeKey(attr)} > ${compileValue(value)}")
        case GreaterThanOrEqual(attr, value) => filter.append(s" ${escapeKey(attr)} >= ${compileValue(value)}")
        case LessThan(attr, value) => filter.append(s" ${escapeKey(attr)} < ${compileValue(value)}")
        case LessThanOrEqual(attr, value) => filter.append(s" ${escapeKey(attr)} <= ${compileValue(value)}")
        case IsNull(attr) => filter.append(s" ${escapeKey(attr)} IS NULL")
        case IsNotNull(attr) => filter.append(s" ${escapeKey(attr)} IS NOT NULL")
        case In(attr, values) => filter.append(s" ${escapeKey(attr)} IN ${values.map(compileValue).mkString("(", ",", ")")}")
        case StringStartsWith(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue(value + "%")}")
        case StringEndsWith(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue("%" + value)}")
        case StringContains(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue("%" + value + "%")}")
        case _ => unsupportedFilters :+ f
      }

      i = i + 1
    })

    (filter.toString(), unsupportedFilters, filters diff unsupportedFilters)
  }

  // ---------- tender: values of nested types: array literals, JSON, scalars in them ----------

  private val jsonFactory = new JsonFactory()
  private lazy val jsonMapper = new ObjectMapper()

  /** A timestamp (microseconds since the epoch) as Cloudberry reads it, in the DB time zone, microseconds kept */
  private def timestampText(micros: Long): String = timestampParseFormatTz.format(Instant.EPOCH.plus(micros, ChronoUnit.MICROS))

  /** A date (days since the epoch); before: days * 3600 * 24 in Int overflowed for dates after 2038-01-19 */
  private def dateText(days: Int): String = dateParseFormat.format(LocalDate.ofEpochDay(days))

  /** tender: a timestamp_ntz (microseconds of the local date-time since 1970-01-01 00:00) — no time zone */
  private def localTimestampText(micros: Long): String =
    timestampParseFormat.format(LocalDateTime.ofEpochSecond(Math.floorDiv(micros, 1000000L),
      (Math.floorMod(micros, 1000000L) * 1000).toInt, ZoneOffset.UTC))

  private def localTimestampMicros(txt: String): Long = {
    val t = LocalDateTime.parse(if (txt.length > 19) rightPadWithChar(txt, 26, '0') else txt, timestampParseFormat)
    t.toEpochSecond(ZoneOffset.UTC) * 1000000L + t.getNano / 1000
  }

  private def timestampMicros(txt: String): Long = {
    val data = if (txt.length > 19) rightPadWithChar(txt, 26, '0') else txt
    val zdt = timestampParseFormatTz.parse(data)
    zdt.getLong(ChronoField.INSTANT_SECONDS) * 1000000L + zdt.getLong(ChronoField.MICRO_OF_SECOND)
  }

  /** A scalar (an array element, a map key, a JSON string of a date / timestamp / binary) as text */
  private def scalarText(dataType: DataType, v: Any): String = dataType match {
    case BooleanType => if (v.asInstanceOf[Boolean]) "true" else "false"
    case TimestampType => timestampText(v.asInstanceOf[Long])
    case TimestampNTZType => localTimestampText(v.asInstanceOf[Long])
    case DateType => dateText(v.asInstanceOf[Int])
    case BinaryType => java.util.Base64.getEncoder.encodeToString(v.asInstanceOf[Array[Byte]])
    case _: DecimalType => v.asInstanceOf[Decimal].toJavaBigDecimal.toPlainString
    case t if cbScalarType(t).isDefined => v.toString // numbers, UTF8String
    case t => throw new IllegalArgumentException(s"Unsupported type ${t.catalogString} of a map key")
  }

  /** The Catalyst value of a scalar from its text (an array element, a map key, a JSON value) */
  private def scalarValue(dataType: DataType, s: String): Any = dataType match {
    case StringType => UTF8String.fromString(s)
    case IntegerType => s.trim.toInt
    case LongType => s.trim.toLong
    case ShortType => s.trim.toShort
    case ByteType => s.trim.toByte
    case DoubleType => s.trim.toDouble
    case FloatType => s.trim.toFloat
    case BooleanType => List("true", "t", "1", "y", "yes").contains(s.trim.toLowerCase)
    case t: DecimalType => Decimal(BigDecimal(s.trim), t.precision, t.scale)
    case DateType => dateParseFormat.parse(s.trim).getLong(ChronoField.EPOCH_DAY).toInt
    case TimestampType => timestampMicros(s.trim)
    case TimestampNTZType => localTimestampMicros(s.trim)
    // bytea in an array literal is hex (\x0102), in JSON — base64 (what toJson writes)
    case BinaryType =>
      if (s.startsWith("\\x")) s.drop(2).grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
      else java.util.Base64.getMimeDecoder.decode(s)
    case t => throw new IllegalArgumentException(s"Unsupported type ${t.catalogString} of a value in an array / map / JSON")
  }

  /** A Cloudberry array literal of scalars: every element quoted ("..." with \ and " escaped), NULL unquoted */
  private def cbArrayText(elementType: DataType, a: ArrayData): String = {
    val sb = new java.lang.StringBuilder("{")
    var i = 0
    while (i < a.numElements()) {
      if (i > 0)
        sb.append(',')
      if (a.isNullAt(i))
        sb.append("NULL")
      else
        sb.append('"').append(scalarText(elementType, a.get(i, elementType)).replace("\\", "\\\\").replace("\"", "\\\"")).append('"')
      i += 1
    }
    sb.append('}').toString
  }

  /**
   * An array from a Cloudberry array literal: {1,2,NULL}, {"a b","c\"d"}, with dimensions ([0:1]={...}), nested
   * ({{1,2},{3,4}}) for an array of arrays
   */
  private def cbArrayValue(elementType: DataType, s: String): ArrayData = {
    var pos = s.indexOf('{')
    if (pos < 0)
      throw new SQLException(s"Not an array literal: ${s.take(200)}")
    def skipSpaces(): Unit = while (pos < s.length && Character.isWhitespace(s.charAt(pos))) pos += 1
    def parse(et: DataType): ArrayData = {
      pos += 1 // {
      val values = scala.collection.mutable.ArrayBuffer[Any]()
      skipSpaces()
      if (s.charAt(pos) == '}') {
        pos += 1
        return new GenericArrayData(values.toArray)
      }
      var more = true
      while (more) {
        skipSpaces()
        s.charAt(pos) match {
          case '{' => et match {
            case at: ArrayType => values += parse(at.elementType)
            case t => throw new SQLException(s"A multidimensional array for the type array<${t.catalogString}>: ${s.take(200)}")
          }
          case '"' =>
            val sb = new java.lang.StringBuilder()
            pos += 1
            while (s.charAt(pos) != '"') {
              if (s.charAt(pos) == '\\')
                pos += 1
              sb.append(s.charAt(pos))
              pos += 1
            }
            pos += 1
            values += scalarValue(et, sb.toString)
          case _ =>
            val start = pos
            while (s.charAt(pos) != ',' && s.charAt(pos) != '}')
              pos += 1
            val token = s.substring(start, pos).trim
            values += (if (token.equalsIgnoreCase("NULL")) null else scalarValue(et, token))
        }
        skipSpaces()
        more = s.charAt(pos) == ','
        pos += 1 // , or }
      }
      new GenericArrayData(values.toArray)
    }
    parse(elementType)
  }

  /** JSON of a value of a nested type: a struct — an object, a map — an object with the keys as text, an array */
  private def jsonText(dataType: DataType, v: Any): String = {
    val writer = new StringWriter()
    val g = jsonFactory.createGenerator(writer)
    def write(dt: DataType, v: Any): Unit = dt match {
      case st: StructType =>
        val r = v.asInstanceOf[InternalRow]
        g.writeStartObject()
        st.fields.zipWithIndex.foreach { case (f, i) =>
          g.writeFieldName(f.name)
          if (r.isNullAt(i)) g.writeNull() else write(f.dataType, r.get(i, f.dataType))
        }
        g.writeEndObject()
      case mt: MapType =>
        val m = v.asInstanceOf[MapData]
        val (keys, values) = (m.keyArray(), m.valueArray())
        g.writeStartObject()
        for (i <- 0 until m.numElements()) {
          g.writeFieldName(scalarText(mt.keyType, keys.get(i, mt.keyType)))
          if (values.isNullAt(i)) g.writeNull() else write(mt.valueType, values.get(i, mt.valueType))
        }
        g.writeEndObject()
      case at: ArrayType =>
        val a = v.asInstanceOf[ArrayData]
        g.writeStartArray()
        for (i <- 0 until a.numElements())
          if (a.isNullAt(i)) g.writeNull() else write(at.elementType, a.get(i, at.elementType))
        g.writeEndArray()
      case StringType => g.writeString(v.toString)
      case IntegerType | LongType | ShortType | ByteType => g.writeNumber(v.asInstanceOf[Number].longValue())
      case DoubleType => g.writeNumber(v.asInstanceOf[Double])
      case FloatType => g.writeNumber(v.asInstanceOf[Float])
      case BooleanType => g.writeBoolean(v.asInstanceOf[Boolean])
      case _: DecimalType => g.writeNumber(v.asInstanceOf[Decimal].toJavaBigDecimal)
      case t => g.writeString(scalarText(t, v)) // date, timestamp, binary (base64)
    }
    write(dataType, v)
    g.close()
    writer.toString
  }

  /** A value of a nested type from JSON; a struct field by its name (exact, else ignoring case), missing — null */
  private def jsonValue(dataType: DataType, node: JsonNode): Any = {
    if (node == null || node.isNull)
      return null
    dataType match {
      case st: StructType =>
        val row = new GenericInternalRow(st.length)
        st.fields.zipWithIndex.foreach { case (f, i) =>
          val field = Option(node.get(f.name)).getOrElse(
            node.fields().asScala.find(_.getKey.equalsIgnoreCase(f.name)).map(_.getValue).orNull)
          row.update(i, jsonValue(f.dataType, field))
        }
        row
      case mt: MapType =>
        val entries = node.fields().asScala.toSeq
        new ArrayBasedMapData(new GenericArrayData(entries.map(e => scalarValue(mt.keyType, e.getKey)).toArray),
          new GenericArrayData(entries.map(e => jsonValue(mt.valueType, e.getValue)).toArray))
      case at: ArrayType => new GenericArrayData(node.elements().asScala.map(jsonValue(at.elementType, _)).toArray)
      case StringType => UTF8String.fromString(if (node.isValueNode) node.asText() else node.toString)
      case t => scalarValue(t, node.asText())
    }
  }

  def internalRowToText(schema: StructType, row: InternalRow, fieldDelimiter: Char = '\t'): String = {
    if (schema.fields.length != row.numFields)
      throw new SQLException(s"internalRowToText: schema.size=${schema.fields.length}, but ${row.numFields} data columns received")
    val ret: StringBuilder = new StringBuilder("")
    schema.fields.zipWithIndex.foreach {
      case (field, i) =>
        if (i > 0)
          ret.append(fieldDelimiter)
        ret.append(if (row.isNullAt(i)) SparkSchemaUtil.TEXT_NULL else fieldText(field.dataType, row, i, fieldDelimiter))
    }
    ret.toString()
  }

  /**
   * tender: a field (not NULL) of the row in the TEXT format, escaped, as internalRowToText writes it — a write
   * formats the bytes with TextRowWriter now (internalRowToText is the reference it is checked against, this one
   * its fallback field by field)
   */
  def fieldText(dataType: DataType, row: InternalRow, i: Int, fieldDelimiter: Char = '\t'): String = dataType match {
    case StringType => escapeText(row.getString(i), fieldDelimiter)
    case a: ArrayType if isCbArray(a) => escapeText(cbArrayText(a.elementType, row.getArray(i)), fieldDelimiter)
    case t if isJsonType(t) => escapeText(jsonText(t, row.get(i, t)), fieldDelimiter)
    case DecimalType.Fixed(p, s) => row.getDecimal(i, p, s).toString()
    case DoubleType => row.getDouble(i).toString
    case FloatType => row.getFloat(i).toString
    case IntegerType => row.getInt(i).toString
    case LongType => row.getLong(i).toString
    case ShortType => row.getShort(i).toString
    case ByteType => row.getByte(i).toString
    case BooleanType => if (row.getBoolean(i)) "1" else "0"
    case TimestampType => timestampText(row.getLong(i))
    case TimestampNTZType => localTimestampText(row.getLong(i))
    case DateType => dateText(row.getInt(i))
    case BinaryType => new String(java.util.Base64.getEncoder.encode(row.getBinary(i)))
    case _ => throw new IllegalArgumentException(s"Unsupported type ${dataType.catalogString}")
  }

  /**
   * A row from the fields of a line of the TEXT format (split by the delimiter). tender: NULL is \N (an empty
   * string and "NULL" are values — before both were read as NULL); text values are unescaped by the TEXT
   * format rules (before: StringEscapeUtils.unescapeJava); arrays — Cloudberry array literals or JSON arrays,
   * maps and structs — JSON (json / jsonb columns read with such a schema).
   *
   * tender: a read parses the bytes with TextRowParser now (this one — the reference it is checked against, and
   * its fallback field by field: setTextField)
   */
  def textToInternalRow(schema: StructType, fields: Array[String]): InternalRow = {
    if (schema.fields.length != fields.length)
      throw new SQLException(s"textToInternalRow: schema.size=${schema.fields.length}, but ${fields.length} data columns received")
    val row = new SpecificInternalRow(schema.fields.map(x => x.dataType))
    fields.zipWithIndex.foreach { case (txt, i) =>
      if (txt == SparkSchemaUtil.TEXT_NULL)
        row.setNullAt(i)
      else
        setTextField(row, i, schema.fields(i).dataType, txt)
    }
    row
  }

  /** tender: a field of the TEXT format (not NULL — not \N) into the row, as textToInternalRow did it */
  def setTextField(row: InternalRow, i: Int, dataType: DataType, txt: String): Unit = dataType match {
    case StringType => row.update(i, UTF8String.fromString(unescapeText(txt)))
    case a: ArrayType =>
      val s = unescapeText(txt)
      row.update(i, if (s.startsWith("[")) jsonValue(a, jsonMapper.readTree(s)) else cbArrayValue(a.elementType, s))
    case t@(_: MapType | _: StructType) => row.update(i, jsonValue(t, jsonMapper.readTree(unescapeText(txt))))
    case DoubleType => row.setDouble(i, txt.toDouble)
    case FloatType => row.setFloat(i, txt.toFloat)
    case IntegerType => row.setInt(i, txt.toInt)
    case LongType => row.setLong(i, txt.toLong)
    case ShortType => row.setShort(i, txt.toShort)
    case ByteType => row.update(i, txt.toByte)
    case TimestampType =>
      try {
        row.setLong(i, timestampMicros(txt))
      } catch {
        case e: java.time.format.DateTimeParseException =>
          throw new SQLException(s"col=${i.toString}: invalid timestamp string $txt")
      }
    case TimestampNTZType => row.setLong(i, localTimestampMicros(txt))
    case DateType => row.setInt(i, dateParseFormat.parse(txt).getLong(ChronoField.EPOCH_DAY).toInt)
    case BooleanType => row.setBoolean(i, List("true", "t", "1", "y", "yes").contains(txt.toLowerCase))
    case DecimalType.Fixed(p, s) => row.update(i, Decimal(BigDecimal.apply(txt), p, s))
    // tender: encode(..., 'base64') of Cloudberry breaks lines every 76 characters (\n in the TEXT format)
    case BinaryType => row.update(i, java.util.Base64.getMimeDecoder.decode(unescapeText(txt)))
    case _ => throw new IllegalArgumentException(s"Unsupported type ${dataType.catalogString}")
  }
}

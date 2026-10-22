package org.apache.spark.sql.tender.cloudberry.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.time.{Instant, LocalDateTime, ZoneOffset, ZonedDateTime}

/**
 * tender: the rows of the TEXT format of a read straight from the bytes the segments send. Before
 * (SparkSchemaUtil.textToInternalRow), every row was decoded into a String and split by a regex into Strings,
 * every string field unescaped and encoded back into UTF-8, every date / timestamp parsed by a DateTimeFormatter,
 * a row object per row, the rows of a chunk queued — 3/4 of the time of a read task (a profile of a 20M-row read).
 *
 * Now the fields are found at the tab bytes (a tab of a value is always escaped, \t); a string without escapes is
 * a copy of its bytes, with escapes — unescaped byte by byte (\NNN and \xHH — a byte, as PostgreSQL takes them);
 * integers, decimals of up to 18 digits, booleans, dates and timestamps of DateStyle ISO (years 1..9999) — from the
 * bytes. Anything else — arrays, JSON, bytea, a number of 19 digits, a decimal to round, infinity, a BC date —
 * goes as its text to SparkSchemaUtil.setTextField, as before.
 *
 * One row object for all the rows, as Spark JDBC does: the scan copies it into an UnsafeRow before it asks for the
 * next one; the values of the row (strings, decimals) are new objects every row, never views of the buffer.
 */
final class TextRowParser(schema: StructType, schemaUtil: SparkSchemaUtil) {
  import TextRowParser._

  private val types: Array[DataType] = schema.fields.map(_.dataType)
  private val n: Int = types.length
  private val kinds: Array[Int] = types.map {
    case StringType => STRING
    case IntegerType => INT
    case LongType => LONG
    case ShortType => SHORT
    case ByteType => BYTE
    case DoubleType => DOUBLE
    case FloatType => FLOAT
    case BooleanType => BOOLEAN
    case DateType => DATE
    case TimestampType => TIMESTAMP
    case TimestampNTZType => TIMESTAMP_NTZ
    case _: DecimalType => DECIMAL
    case _ => OTHER
  }
  private val precisions: Array[Int] = types.map { case d: DecimalType => d.precision; case _ => 0 }
  private val scales: Array[Int] = types.map { case d: DecimalType => d.scale; case _ => 0 }
  private val row = new SpecificInternalRow(types)
  /** the offset of the DB time zone when it has one only (UTC, +03:00), else the rules resolve every timestamp */
  private val fixedOffsetSeconds: Option[Int] = {
    val rules = schemaUtil.dbTimeZoneId.getRules
    if (rules.isFixedOffset) Some(rules.getOffset(Instant.EPOCH).getTotalSeconds) else None
  }

  /** after parseLine: the position after the LF of the line */
  private var lineEnd: Int = 0

  /** where the line of the last parseLine ends: the position after its LF */
  def nextLine: Int = lineEnd

  /**
   * The row of the line at start of a buffer of LF-terminated lines: the fields and the LF found in one pass
   * (a tab and an LF of a value are always escaped); the next line — at nextLine
   */
  def parseLine(b: Array[Byte], start: Int): InternalRow = {
    var pos = start
    var i = 0
    var c: Byte = '\t'
    while (i < n && c == '\t') {
      var e = pos
      while (b(e) != '\t' && b(e) != '\n')
        e += 1
      c = b(e)
      setField(i, b, pos, e)
      pos = e + 1
      i += 1
    }
    if (i < n || c != '\n') {
      var eol = pos - 1
      while (b(eol) != '\n')
        eol += 1
      throw columnsMismatch(b, start, eol)
    }
    lineEnd = pos
    row
  }

  private def columnsMismatch(b: Array[Byte], start: Int, end: Int): SQLException = {
    var fields = 1
    var k = start
    while (k < end) {
      if (b(k) == '\t')
        fields += 1
      k += 1
    }
    new SQLException(s"textToInternalRow: schema.size=$n, but $fields data columns received")
  }

  private def setField(i: Int, b: Array[Byte], s: Int, e: Int): Unit = {
    if (e - s == 2 && b(s) == '\\' && b(s + 1) == 'N') {
      row.setNullAt(i)
      return
    }
    val parsed = kinds(i) match {
      case STRING =>
        row.update(i, utf8(b, s, e))
        true
      case INT =>
        val v = parseLong(b, s, e)
        (v != NOT_PARSED && v >= Int.MinValue && v <= Int.MaxValue) && { row.setInt(i, v.toInt); true }
      case LONG =>
        val v = parseLong(b, s, e)
        v != NOT_PARSED && { row.setLong(i, v); true }
      case SHORT =>
        val v = parseLong(b, s, e)
        (v != NOT_PARSED && v >= Short.MinValue && v <= Short.MaxValue) && { row.setShort(i, v.toShort); true }
      case BYTE =>
        val v = parseLong(b, s, e)
        (v != NOT_PARSED && v >= Byte.MinValue && v <= Byte.MaxValue) && { row.setByte(i, v.toByte); true }
      case DOUBLE =>
        row.setDouble(i, java.lang.Double.parseDouble(ascii(b, s, e)))
        true
      case FLOAT =>
        row.setFloat(i, java.lang.Float.parseFloat(ascii(b, s, e)))
        true
      case BOOLEAN =>
        row.setBoolean(i, parseBoolean(b, s, e))
        true
      case DATE =>
        val day = epochDay(b, s, e)
        day != NOT_PARSED && { row.setInt(i, day.toInt); true }
      case TIMESTAMP =>
        val micros = timestampMicros(b, s, e, local = false)
        micros != NOT_PARSED && { row.setLong(i, micros); true }
      case TIMESTAMP_NTZ =>
        val micros = timestampMicros(b, s, e, local = true)
        micros != NOT_PARSED && { row.setLong(i, micros); true }
      case DECIMAL =>
        val d = decimal(b, s, e, precisions(i), scales(i))
        (d != null) && { row.update(i, d); true }
      case _ => false
    }
    if (!parsed)
      schemaUtil.setTextField(row, i, types(i), new String(b, s, e - s, StandardCharsets.UTF_8))
  }

  /** A string: a copy of its bytes, unescaped when it has a backslash */
  private def utf8(b: Array[Byte], s: Int, e: Int): UTF8String = {
    var k = s
    while (k < e && b(k) != '\\')
      k += 1
    if (k == e) UTF8String.fromBytes(java.util.Arrays.copyOfRange(b, s, e))
    else UTF8String.fromBytes(unescape(b, s, e, k))
  }

  /** The TEXT format escapes (\b \f \n \r \t \v, \NNN, \xHH, \c — c) of [s, e), the first one at firstBackslash */
  private def unescape(b: Array[Byte], s: Int, e: Int, firstBackslash: Int): Array[Byte] = {
    val out = new Array[Byte](e - s)
    System.arraycopy(b, s, out, 0, firstBackslash - s)
    var o = firstBackslash - s
    var k = firstBackslash
    while (k < e) {
      val c = b(k)
      if (c != '\\' || k + 1 == e) {
        out(o) = c
        k += 1
      } else {
        val x = b(k + 1)
        k += 2
        out(o) = x match {
          case 'b' => 8
          case 'f' => 12
          case 'n' => 10
          case 'r' => 13
          case 't' => 9
          case 'v' => 11
          case d if d >= '0' && d <= '7' =>
            var code = d - '0'
            var digits = 1
            while (digits < 3 && k < e && b(k) >= '0' && b(k) <= '7') {
              code = code * 8 + (b(k) - '0')
              k += 1
              digits += 1
            }
            code.toByte
          case 'x' if k < e && hexDigit(b(k)) >= 0 =>
            var code = hexDigit(b(k))
            k += 1
            if (k < e && hexDigit(b(k)) >= 0) {
              code = code * 16 + hexDigit(b(k))
              k += 1
            }
            code.toByte
          case other => other
        }
      }
      o += 1
    }
    java.util.Arrays.copyOf(out, o)
  }

  private def hexDigit(c: Byte): Int =
    if (c >= '0' && c <= '9') c - '0'
    else if (c >= 'a' && c <= 'f') c - 'a' + 10
    else if (c >= 'A' && c <= 'F') c - 'A' + 10
    else -1

  /** A number has ASCII characters only (a Latin-1 String is a copy of its bytes) */
  private def ascii(b: Array[Byte], s: Int, e: Int): String = new String(b, s, e - s, StandardCharsets.ISO_8859_1)

  /** An integer of up to 18 digits with an optional sign (no overflow possible); NOT_PARSED — anything else */
  private def parseLong(b: Array[Byte], s: Int, e: Int): Long = {
    var p = s
    val negative = p < e && b(p) == '-'
    if (negative || (p < e && b(p) == '+'))
      p += 1
    if (e - p < 1 || e - p > 18)
      return NOT_PARSED
    var v = 0L
    while (p < e) {
      val d = b(p) - '0'
      if (d < 0 || d > 9)
        return NOT_PARSED
      v = v * 10 + d
      p += 1
    }
    if (negative) -v else v
  }

  /** As before: true, t, 1, y, yes in any case */
  private def parseBoolean(b: Array[Byte], s: Int, e: Int): Boolean = e - s match {
    case 1 => val c = b(s) | 0x20; c == 't' || c == 'y' || b(s) == '1'
    case 3 => lowerEquals(b, s, "yes")
    case 4 => lowerEquals(b, s, "true")
    case _ => false
  }

  private def lowerEquals(b: Array[Byte], s: Int, word: String): Boolean = {
    var k = 0
    while (k < word.length && (b(s + k) | 0x20) == word.charAt(k))
      k += 1
    k == word.length
  }

  private def digit(b: Array[Byte], p: Int): Int = {
    val d = b(p) - '0'
    if (d >= 0 && d <= 9) d else -1000000
  }

  /** A date yyyy-MM-dd (years 1..9999) as days since 1970-01-01; NOT_PARSED — anything else */
  private def epochDay(b: Array[Byte], s: Int, e: Int): Long =
    if (e - s != 10 || b(s + 4) != '-' || b(s + 7) != '-') NOT_PARSED
    else epochDayAt(b, s)

  private def epochDayAt(b: Array[Byte], s: Int): Long = {
    val y = digit(b, s) * 1000 + digit(b, s + 1) * 100 + digit(b, s + 2) * 10 + digit(b, s + 3)
    val m = digit(b, s + 5) * 10 + digit(b, s + 6)
    val d = digit(b, s + 8) * 10 + digit(b, s + 9)
    if (y < 1 || m < 1 || m > 12 || d < 1 || d > monthLength(y, m)) NOT_PARSED
    else daysSinceEpoch(y, m, d)
  }

  /**
   * A timestamp yyyy-MM-dd HH:mm:ss[.f{1,6}] as microseconds since the epoch: of the instant in the DB time zone
   * (gaps and overlaps resolved as ZonedDateTime.ofLocal, as the DateTimeFormatter before) or, local, of the local
   * date-time; NOT_PARSED — anything else
   */
  private def timestampMicros(b: Array[Byte], s: Int, e: Int, local: Boolean): Long = {
    val len = e - s
    if (len < 19 || len > 26 || (len > 19 && (len == 20 || b(s + 19) != '.')) || b(s + 4) != '-' || b(s + 7) != '-' ||
      b(s + 10) != ' ' || b(s + 13) != ':' || b(s + 16) != ':')
      return NOT_PARSED
    val day = epochDayAt(b, s)
    val hh = digit(b, s + 11) * 10 + digit(b, s + 12)
    val mi = digit(b, s + 14) * 10 + digit(b, s + 15)
    val ss = digit(b, s + 17) * 10 + digit(b, s + 18)
    if (day == NOT_PARSED || hh < 0 || hh > 23 || mi < 0 || mi > 59 || ss < 0 || ss > 59)
      return NOT_PARSED
    var fraction = 0L
    var p = s + 20
    while (p < e) {
      val d = digit(b, p)
      if (d < 0)
        return NOT_PARSED
      fraction = fraction * 10 + d
      p += 1
    }
    var k = math.max(0, len - 20)
    while (k < 6) {
      fraction *= 10
      k += 1
    }
    val localSeconds = day * 86400L + hh * 3600 + mi * 60 + ss
    val seconds =
      if (local) localSeconds
      else fixedOffsetSeconds match {
        case Some(offset) => localSeconds - offset
        case None => ZonedDateTime.ofLocal(LocalDateTime.ofEpochSecond(localSeconds, 0, ZoneOffset.UTC),
          schemaUtil.dbTimeZoneId, null).toEpochSecond
      }
    seconds * 1000000L + fraction
  }

  /**
   * A decimal of up to 18 digits that needs no rounding to the scale, within the precision; null — anything else
   * (the slow path rounds HALF_UP or fails, as before)
   */
  private def decimal(b: Array[Byte], s: Int, e: Int, precision: Int, scale: Int): Decimal = {
    var p = s
    val negative = p < e && b(p) == '-'
    if (negative || (p < e && b(p) == '+'))
      p += 1
    var v = 0L
    var digits = 0
    var fractionDigits = -1
    while (p < e) {
      val c = b(p)
      if (c == '.') {
        if (fractionDigits >= 0)
          return null
        fractionDigits = 0
      } else {
        val d = c - '0'
        if (d < 0 || d > 9)
          return null
        v = v * 10 + d
        digits += 1
        if (fractionDigits >= 0)
          fractionDigits += 1
        if (digits > 18)
          return null
      }
      p += 1
    }
    val f = math.max(fractionDigits, 0)
    if (digits == 0 || f > scale || digits + scale - f > 18)
      return null
    var k = f
    while (k < scale) {
      v *= 10
      k += 1
    }
    new Decimal().setOrNull(if (negative) -v else v, precision, scale)
  }
}

object TextRowParser {
  private final val STRING = 1
  private final val INT = 2
  private final val LONG = 3
  private final val SHORT = 4
  private final val BYTE = 5
  private final val DOUBLE = 6
  private final val FLOAT = 7
  private final val BOOLEAN = 8
  private final val DATE = 9
  private final val TIMESTAMP = 10
  private final val TIMESTAMP_NTZ = 11
  private final val DECIMAL = 12
  private final val OTHER = 0
  /** the result of a fast path that does not take the text */
  private final val NOT_PARSED = Long.MinValue

  private def isLeap(y: Int): Boolean = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0

  private def monthLength(y: Int, m: Int): Int = m match {
    case 2 => if (isLeap(y)) 29 else 28
    case 4 | 6 | 9 | 11 => 30
    case _ => 31
  }

  /** java.time.LocalDate.toEpochDay of a valid date of the years 1..9999 */
  private def daysSinceEpoch(y: Int, m: Int, d: Int): Long = {
    var total = 365L * y + (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400
    total += (367 * m - 362) / 12
    total += d - 1
    if (m > 2) {
      total -= 1
      if (!isLeap(y))
        total -= 1
    }
    total - 719528L // days from 0000-01-01 to 1970-01-01
  }
}

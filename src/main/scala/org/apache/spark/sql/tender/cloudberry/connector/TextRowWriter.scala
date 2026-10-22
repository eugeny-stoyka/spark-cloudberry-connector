package org.apache.spark.sql.tender.cloudberry.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * tender: the rows of a write in the TEXT format straight into bytes — a block of lines the task hands to its
 * channel at once. Before (SparkSchemaUtil.internalRowToText), every row was a StringBuilder: the strings decoded
 * from UTF-8 into String and escaped, every date / timestamp formatted by a DateTimeFormatter, the line encoded
 * back into UTF-8 and put into the channel row by row — ~60% of the time of a write task (a profile of a 20M-row
 * write).
 *
 * Now a string goes as its UTF-8 bytes, escaped byte by byte (the backslash, LF, CR, the tab), checked to be
 * valid UTF-8 on the way; integers, booleans, decimals of up to 18 digits (the plain notation), dates and
 * timestamps of the years 1..9999, binary (base64) — straight into the bytes. Anything else — arrays, JSON,
 * doubles, floats, a string that is not valid UTF-8 (decoded with replacements, as before), a decimal BigDecimal
 * writes with an exponent, a date out of the years 1..9999 — by SparkSchemaUtil.fieldText, as before: the bytes
 * are the same as of internalRowToText + LF.
 */
final class TextRowWriter(schema: StructType, schemaUtil: SparkSchemaUtil) {
  import TextRowWriter._

  private val types: Array[DataType] = schema.fields.map(_.dataType)
  private val n: Int = types.length
  private val kinds: Array[Int] = types.map {
    case StringType => STRING
    case IntegerType => INT
    case LongType => LONG
    case ShortType => SHORT
    case ByteType => BYTE
    case BooleanType => BOOLEAN
    case DateType => DATE
    case TimestampType => TIMESTAMP
    case TimestampNTZType => TIMESTAMP_NTZ
    case _: DecimalType => DECIMAL
    case BinaryType => BINARY
    case _ => OTHER
  }
  private val precisions: Array[Int] = types.map { case d: DecimalType => d.precision; case _ => 0 }
  private val scales: Array[Int] = types.map { case d: DecimalType => d.scale; case _ => 0 }
  /** the offset of the DB time zone when it has one only (UTC, +03:00), else the rules give it for every instant */
  private val zoneRules = schemaUtil.dbTimeZoneId.getRules
  private val fixedOffset: Boolean = zoneRules.isFixedOffset
  private val fixedOffsetSeconds: Int = zoneRules.getOffset(Instant.EPOCH).getTotalSeconds
  private val digits = new Array[Byte](20)

  private var buf = new Array[Byte](InitialCapacity)
  private var pos = 0

  /** the lines written since the last reset: bytes(0 until length) */
  def bytes: Array[Byte] = buf
  def length: Int = pos
  def reset(): Unit = pos = 0

  /** Appends the row as a line: the fields separated by tabs, LF at the end */
  def write(row: InternalRow): Unit = {
    if (row.numFields != n)
      throw new java.sql.SQLException(s"internalRowToText: schema.size=$n, but ${row.numFields} data columns received")
    var i = 0
    while (i < n) {
      if (i > 0)
        put(TAB)
      if (row.isNullAt(i)) {
        put(BACKSLASH)
        put(UPPER_N)
      } else
        writeField(row, i)
      i += 1
    }
    put(LF)
  }

  private def writeField(row: InternalRow, i: Int): Unit = {
    val start = pos
    val written = kinds(i) match {
      case STRING => utf8(row, i)
      case INT => writeLong(row.getInt(i)); true
      case LONG => writeLong(row.getLong(i)); true
      case SHORT => writeLong(row.getShort(i)); true
      case BYTE => writeLong(row.getByte(i)); true
      case BOOLEAN => put(if (row.getBoolean(i)) ONE else ZERO); true
      case DATE => date(row.getInt(i))
      case TIMESTAMP => timestamp(row.getLong(i), local = false)
      case TIMESTAMP_NTZ => timestamp(row.getLong(i), local = true)
      case DECIMAL => decimal(row.getDecimal(i, precisions(i), scales(i)))
      case BINARY => putBytes(java.util.Base64.getEncoder.encode(row.getBinary(i))); true
      case _ => false
    }
    if (!written) {
      pos = start
      putBytes(schemaUtil.fieldText(types(i), row, i).getBytes(StandardCharsets.UTF_8))
    }
  }

  private def ensure(more: Int): Unit =
    if (pos + more > buf.length)
      buf = java.util.Arrays.copyOf(buf, math.max(buf.length * 2, pos + more))

  private def put(b: Byte): Unit = {
    ensure(1)
    buf(pos) = b
    pos += 1
  }

  /** a byte with the capacity ensured by the caller */
  private def append(b: Byte): Unit = {
    buf(pos) = b
    pos += 1
  }

  private def putBytes(b: Array[Byte]): Unit = {
    ensure(b.length)
    System.arraycopy(b, 0, buf, pos, b.length)
    pos += b.length
  }

  /**
   * A string: its UTF-8 bytes with the backslash, LF, CR and the tab escaped (as escapeText); false — not valid
   * UTF-8 (the fallback decodes it with replacements, as before)
   */
  private def utf8(row: InternalRow, i: Int): Boolean = {
    val s = row.getUTF8String(i)
    val base = s.getBaseObject
    val offset = s.getBaseOffset
    val len = s.numBytes()
    ensure(2 * len)
    var k = 0
    while (k < len) {
      val b = Platform.getByte(base, offset + k)
      if (b >= 0) {
        val escaped: Byte =
          if (b == BACKSLASH) BACKSLASH else if (b == LF) LOWER_N else if (b == CR) LOWER_R else if (b == TAB) LOWER_T
          else 0
        if (escaped != 0) {
          append(BACKSLASH)
          append(escaped)
        } else
          append(b)
        k += 1
      } else {
        val sequence = utf8SequenceLength(base, offset, k, len)
        if (sequence == 0)
          return false
        var j = 0
        while (j < sequence) {
          append(Platform.getByte(base, offset + k + j))
          j += 1
        }
        k += sequence
      }
    }
    true
  }

  /** The length of the valid UTF-8 sequence of a non-ASCII byte at k (RFC 3629, as the decoder of Java); 0 — invalid */
  private def utf8SequenceLength(base: AnyRef, offset: Long, k: Int, len: Int): Int = {
    def at(j: Int): Int = if (k + j < len) Platform.getByte(base, offset + k + j) & 0xff else -1
    def continuation(j: Int): Boolean = { val c = at(j); c >= 0x80 && c <= 0xbf }
    val b0 = at(0)
    if (b0 >= 0xc2 && b0 <= 0xdf) {
      if (continuation(1)) 2 else 0
    } else if (b0 >= 0xe0 && b0 <= 0xef) {
      val b1 = at(1)
      val secondValid =
        if (b0 == 0xe0) b1 >= 0xa0 && b1 <= 0xbf // no overlong
        else if (b0 == 0xed) b1 >= 0x80 && b1 <= 0x9f // no surrogates
        else b1 >= 0x80 && b1 <= 0xbf
      if (secondValid && continuation(2)) 3 else 0
    } else if (b0 >= 0xf0 && b0 <= 0xf4) {
      val b1 = at(1)
      val secondValid =
        if (b0 == 0xf0) b1 >= 0x90 && b1 <= 0xbf // no overlong
        else if (b0 == 0xf4) b1 >= 0x80 && b1 <= 0x8f // up to U+10FFFF
        else b1 >= 0x80 && b1 <= 0xbf
      if (secondValid && continuation(2) && continuation(3)) 4 else 0
    } else 0
  }

  /** An integer in decimal digits, as Long.toString */
  private def writeLong(v: Long): Unit = {
    if (v == Long.MinValue) {
      putBytes(LONG_MIN)
      return
    }
    ensure(20)
    var x = v
    if (x < 0) {
      append(MINUS)
      x = -x
    }
    var d = 0
    do {
      digits(d) = (ZERO + x % 10).toByte
      x /= 10
      d += 1
    } while (x > 0)
    while (d > 0) {
      d -= 1
      append(digits(d))
    }
  }

  /** two digits, the capacity ensured by the caller */
  private def append2(v: Int): Unit = {
    append((ZERO + v / 10).toByte)
    append((ZERO + v % 10).toByte)
  }

  /**
   * yyyy-MM-dd of days since 1970-01-01 (the proleptic Gregorian calendar, H. Hinnant's civil_from_days); false —
   * a year out of 1..9999
   */
  private def date(days: Long): Boolean = {
    val z = days + 719468L
    val era = Math.floorDiv(z, 146097L)
    val doe = z - era * 146097L
    val yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
    val doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
    val mp = (5 * doy + 2) / 153
    val d = (doy - (153 * mp + 2) / 5 + 1).toInt
    val m = (if (mp < 10) mp + 3 else mp - 9).toInt
    val y = yoe + era * 400 + (if (m <= 2) 1 else 0)
    if (y < 1 || y > 9999)
      return false
    ensure(10)
    append2((y / 100).toInt)
    append2((y % 100).toInt)
    append(MINUS)
    append2(m)
    append(MINUS)
    append2(d)
    true
  }

  /**
   * yyyy-MM-dd HH:mm:ss.SSSSSS of microseconds since the epoch: in the DB time zone, or, local, of the local
   * date-time (as the DateTimeFormatter of the fallback: all 6 digits of the fraction); false — a year out of 1..9999
   */
  private def timestamp(micros: Long, local: Boolean): Boolean = {
    val seconds = Math.floorDiv(micros, 1000000L)
    val fraction = Math.floorMod(micros, 1000000L)
    val localSeconds =
      if (local) seconds
      else if (fixedOffset) seconds + fixedOffsetSeconds
      else seconds + zoneRules.getOffset(Instant.ofEpochSecond(seconds)).getTotalSeconds
    if (!date(Math.floorDiv(localSeconds, 86400L)))
      return false
    ensure(16)
    val secondOfDay = Math.floorMod(localSeconds, 86400L).toInt
    append(SPACE)
    append2(secondOfDay / 3600)
    append(COLON)
    append2(secondOfDay / 60 % 60)
    append(COLON)
    append2(secondOfDay % 60)
    append(DOT)
    var p = 100000L
    while (p > 0) {
      append((ZERO + fraction / p % 10).toByte)
      p /= 10
    }
    true
  }

  /**
   * A decimal of up to 18 digits as BigDecimal.toString writes it in the plain notation (the scale >= 0, the
   * exponent of the first digit >= -6); false — anything else (an exponent or more digits — the fallback)
   */
  private def decimal(d: Decimal): Boolean = {
    if (d.precision > 18 || d.scale < 0)
      return false
    val unscaled = d.toUnscaledLong
    val scale = d.scale
    var magnitude = math.abs(unscaled)
    var count = 0
    do {
      digits(count) = (ZERO + magnitude % 10).toByte
      magnitude /= 10
      count += 1
    } while (magnitude > 0)
    if (count - 1 - scale < -6)
      return false
    ensure(count + scale + 3)
    // the digits are in digits(count - 1) .. digits(0), the most significant first; the last `scale` of them
    // are the fraction
    if (unscaled < 0)
      append(MINUS)
    if (count <= scale) { // 0.00ddd
      append(ZERO)
      append(DOT)
      var zeros = scale - count
      while (zeros > 0) {
        append(ZERO)
        zeros -= 1
      }
    } else {
      while (count > scale) { // the integer part
        count -= 1
        append(digits(count))
      }
      if (scale > 0)
        append(DOT)
    }
    while (count > 0) {
      count -= 1
      append(digits(count))
    }
    true
  }
}

object TextRowWriter {
  private final val STRING = 1
  private final val INT = 2
  private final val LONG = 3
  private final val SHORT = 4
  private final val BYTE = 5
  private final val BOOLEAN = 6
  private final val DATE = 7
  private final val TIMESTAMP = 8
  private final val TIMESTAMP_NTZ = 9
  private final val DECIMAL = 10
  private final val BINARY = 11
  private final val OTHER = 0

  private final val TAB: Byte = 9
  private final val LF: Byte = 10
  private final val CR: Byte = 13
  private final val SPACE: Byte = 32
  private final val MINUS: Byte = 45
  private final val DOT: Byte = 46
  private final val ZERO: Byte = 48
  private final val ONE: Byte = 49
  private final val COLON: Byte = 58
  private final val UPPER_N: Byte = 78
  private final val BACKSLASH: Byte = 92
  private final val LOWER_N: Byte = 110
  private final val LOWER_R: Byte = 114
  private final val LOWER_T: Byte = 116
  private val LONG_MIN: Array[Byte] = Long.MinValue.toString.getBytes(StandardCharsets.US_ASCII)

  /** a task hands its lines to the channel in blocks of about that size */
  final val BlockBytes = 64 * 1024
  private final val InitialCapacity = 2 * BlockBytes
}

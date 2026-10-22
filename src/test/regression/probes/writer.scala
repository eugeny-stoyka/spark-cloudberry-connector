// TextRowWriter (the rows of a write straight into bytes, in blocks) against SparkSchemaUtil.internalRowToText
// (the String of a row before it, its fallback) + LF in UTF-8: random rows of all the types written — strings with
// escapes, emoji and broken UTF-8 (overlong, surrogates, above U+10FFFF, cut sequences), the edges of the integers,
// NaN / -0.0, dates and timestamps also out of the years 1..9999 and at the DST changes, decimals BigDecimal writes
// with an exponent, arrays, maps, binary, NULL — as UnsafeRow (what Spark passes) and GenericInternalRow, in 4 DB
// time zones; the same bytes row by row and for a block of 1000 rows
:load /regression/probes/common.scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.tender.cloudberry.connector.{SparkSchemaUtil, TextRowWriter}
import org.apache.spark.unsafe.types.UTF8String
import java.nio.charset.StandardCharsets.UTF_8
val schema = StructType(Seq(StructField("s", StringType), StructField("i", IntegerType), StructField("l", LongType),
  StructField("sh", ShortType), StructField("by", ByteType), StructField("d", DoubleType), StructField("f", FloatType),
  StructField("bo", BooleanType), StructField("dt", DateType), StructField("ts", TimestampType),
  StructField("ntz", TimestampNTZType), StructField("dec", DecimalType(12, 2)), StructField("dec18", DecimalType(18, 10)),
  StructField("dec38", DecimalType(38, 18)), StructField("dec5", DecimalType(5, 0)), StructField("arr", ArrayType(IntegerType)),
  StructField("m", MapType(StringType, IntegerType)), StructField("bin", BinaryType)))
val rnd = new scala.util.Random(20261022)
def pick[T](xs: Seq[T]): T = xs(rnd.nextInt(xs.size))
def bytes(xs: Int*): UTF8String = UTF8String.fromBytes(xs.map(_.toByte).toArray)
val strings = Seq("", "NULL", "\\N", "привет мир", "tab\there", "line\nbreak", "cr\rhere", "back\\slash", "quote\"d",
  "日本語 🎉", "x" * 300, "\u0000\u0001\b\f\u000b")
def randomString(): UTF8String = pick(Seq(
  UTF8String.fromString(pick(strings)), UTF8String.fromString(rnd.alphanumeric.take(rnd.nextInt(60)).mkString),
  UTF8String.fromString(new String(Array.fill(rnd.nextInt(20))(rnd.nextInt(0x3000).toChar).filterNot(Character.isSurrogate))),
  bytes(0xc3), bytes(0x61, 0xed, 0xa0, 0x80), bytes(0xf4, 0x90, 0x80, 0x80), bytes(0xe0, 0x80, 0x80), bytes(0x80, 0x61),
  bytes(0x6f, 0x6b, 0xff), bytes(0xc0, 0xaf), bytes(0xf0, 0x9f, 0x8e), bytes(0xe2, 0x82, 0xac, 0x0a, 0x5c)))
def micros(): Long = pick(Seq(rnd.nextLong() % 400000000000000000L, (rnd.nextDouble() * 4e15).toLong - 1000000000000000L,
  java.time.Instant.parse("2026-03-08T07:30:00Z").toEpochMilli * 1000, java.time.Instant.parse("2026-11-01T05:30:15.000123Z").toEpochMilli * 1000 + 123,
  java.time.Instant.parse("2011-03-26T23:30:00Z").toEpochMilli * 1000, 0L, -1L, 253402300799999999L, 253402300800000000L, -62135596800000000L, -62135596800000001L))
def value(c: Int): Any = if (rnd.nextInt(12) == 0) null else c match {
  case 0 => randomString()
  case 1 => pick(Seq(rnd.nextInt(), Int.MaxValue, Int.MinValue, 0, -1))
  case 2 => pick(Seq(rnd.nextLong(), Long.MaxValue, Long.MinValue, 0L, -10L))
  case 3 => pick(Seq(rnd.nextInt(65536) - 32768, 32767, -32768)).toShort
  case 4 => pick(Seq(rnd.nextInt(256) - 128, 127, -128)).toByte
  case 5 => pick(Seq(rnd.nextDouble(), rnd.nextGaussian() * 1e20, Double.NaN, Double.PositiveInfinity, -0.0, 1e-300, 1e300))
  case 6 => pick(Seq(rnd.nextFloat(), Float.NaN, Float.NegativeInfinity, -0.0f, 3.4e38f))
  case 7 => rnd.nextBoolean()
  case 8 => pick(Seq(rnd.nextInt(4800000) - 1100000, 0, -719162, -719163, 2932896, 2932897, 20455, -1))
  case 9 | 10 => micros()
  case 11 => org.apache.spark.sql.types.Decimal(pick(Seq(rnd.nextLong() % 1000000000000L, 0L, -5L, 999999999999L, 1L)), 12, 2)
  case 12 => org.apache.spark.sql.types.Decimal(pick(Seq(rnd.nextLong() % 1000000000000000000L, 1L, 12345L, -1L, 0L, 100000000L)), 18, 10)
  case 13 => org.apache.spark.sql.types.Decimal(BigDecimal(pick(Seq(BigInt(rnd.nextLong()) * BigInt(rnd.nextLong()), BigInt(1), BigInt(0), BigInt(-123456789)))) / BigDecimal(10).pow(18), 38, 18)
  case 14 => org.apache.spark.sql.types.Decimal(pick(Seq(rnd.nextInt(199999) - 99999, 0, 99999)).toLong, 5, 0)
  case 15 => new GenericArrayData(Array[Any](rnd.nextInt(), null, -1))
  case 16 => ArrayBasedMapData(Array[Any](UTF8String.fromString("k\t1"), UTF8String.fromString("ключ")), Array[Any](1, null))
  case 17 => Array.fill(rnd.nextInt(100))(rnd.nextInt(256).toByte)
}
def compare(zone: String, rows: Int): String = {
  val util = SparkSchemaUtil(zone)
  val writer = new TextRowWriter(schema, util)
  val toUnsafe = UnsafeProjection.create(schema)
  def expected(r: InternalRow): Array[Byte] = (util.internalRowToText(schema, r, '\t') + "\n").getBytes(UTF_8)
  var differences = List.empty[String]
  val block = new java.io.ByteArrayOutputStream()
  val blockWriter = new TextRowWriter(schema, util)
  for (k <- 1 to rows) {
    val generic = new GenericInternalRow(schema.fields.indices.map(value).toArray)
    for (r <- Seq(toUnsafe(generic).copy(), generic)) {
      val e = expected(r)
      writer.reset()
      writer.write(r)
      val a = java.util.Arrays.copyOf(writer.bytes, writer.length)
      if (!java.util.Arrays.equals(a, e) && differences.size < 5)
        differences ::= s"expected [${new String(e, UTF_8)}], got [${new String(a, UTF_8)}]".take(600)
    }
    if (k <= 1000) {
      val u = toUnsafe(generic).copy()
      blockWriter.write(u)
      block.write(expected(u))
    }
  }
  if (!java.util.Arrays.equals(java.util.Arrays.copyOf(blockWriter.bytes, blockWriter.length), block.toByteArray))
    differences ::= s"a block of 1000 rows: ${blockWriter.length} bytes, expected ${block.size()}"
  if (differences.isEmpty) s"$rows rows" else differences.mkString("; ")
}
for (zone <- Seq("UTC", "Europe/Moscow", "America/New_York", "+05:30"))
  check(s"TextRowWriter = internalRowToText, DB time zone $zone", "20000 rows") { compare(zone, 20000) }
done()
:quit

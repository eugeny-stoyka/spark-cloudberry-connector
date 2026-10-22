// TextRowParser (the rows of a read from the bytes, parseLine as the reader calls it) against
// SparkSchemaUtil.textToInternalRow (the parser of the strings before it, its fallback): lines of the TEXT format
// as Cloudberry writes them — escapes, NULL and "\N" as a value, the edges of the integers, doubles, booleans,
// dates of the years 1..9999, timestamps with 0..6 digits of the fraction and the DST gaps / overlaps, decimals
// to round, arrays, bytea — in 4 DB time zones; the same values field by field or the same failure (wrong number
// of fields, a number out of range); parseLine — also where the next line starts
:load /regression/probes/common.scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.tender.cloudberry.connector.{SparkSchemaUtil, TextRowParser}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{LocalDate, LocalDateTime}
val schema = StructType(Seq(StructField("s", StringType), StructField("i", IntegerType), StructField("l", LongType),
  StructField("sh", ShortType), StructField("by", ByteType), StructField("d", DoubleType), StructField("f", FloatType),
  StructField("bo", BooleanType), StructField("dt", DateType), StructField("ts", TimestampType),
  StructField("ntz", TimestampNTZType), StructField("dec", DecimalType(12, 2)), StructField("dec38", DecimalType(38, 18)),
  StructField("dec5", DecimalType(5, 0)), StructField("arr", ArrayType(IntegerType)), StructField("bin", BinaryType)))
val rnd = new scala.util.Random(20261022)
def pick[T](xs: Seq[T]): T = xs(rnd.nextInt(xs.size))
val secondsFormat = java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
// as Cloudberry writes a timestamp: the fraction without its trailing zeros, none for a whole second
def ts(t: LocalDateTime): String = {
  val us = t.getNano / 1000
  t.format(secondsFormat) + (if (us == 0) "" else "." + f"$us%06d".reverse.dropWhile(_ == '0').reverse)
}
def randomTs(): LocalDateTime = pick(Seq(
  LocalDateTime.of(1 + rnd.nextInt(9999), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28), rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60), rnd.nextInt(1000000) * 1000),
  LocalDateTime.of(1960 + rnd.nextInt(80), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28), rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60), pick(Seq(0, 120000000, 500000, 999999000))),
  pick(Seq(LocalDateTime.of(2026, 3, 8, 2, 30), LocalDateTime.of(2026, 11, 1, 1, 30, 15), LocalDateTime.of(2011, 3, 27, 2, 30),
    LocalDateTime.of(1970, 1, 1, 0, 0), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500000000), LocalDateTime.of(2038, 1, 19, 3, 14, 8)))))
val strings = Seq("", "NULL", "\\N", "привет мир", "tab\there", "line\nbreak", "cr\rhere", "back\\slash", "quote\"d", "a b c", "日本語 🎉", "x" * 300)
def field(c: Int): String = if (rnd.nextInt(12) == 0) "\\N" else c match {
  case 0 => pick(Seq(SparkSchemaUtil.escapeText(pick(strings)), "\\b\\f\\v", "\\101\\102", "\\x41z", "end\\", "a\\qb", rnd.alphanumeric.take(rnd.nextInt(40)).mkString))
  case 1 => pick(Seq(rnd.nextInt().toString, "2147483647", "-2147483648", "2147483648", "+5", "007", "0", "-0", "1.5"))
  case 2 => pick(Seq(rnd.nextLong().toString, "9223372036854775807", "-9223372036854775808", "999999999999999999", "12", "-1"))
  case 3 => pick(Seq((rnd.nextInt(65536) - 32768).toString, "32767", "-32768", "32768"))
  case 4 => pick(Seq((rnd.nextInt(256) - 128).toString, "127", "-128", "128"))
  case 5 => pick(Seq(rnd.nextDouble().toString, (rnd.nextGaussian() * 1e20).toString, "Infinity", "-Infinity", "NaN", "1e+20", "-1.5E-7", "0", "-0"))
  case 6 => pick(Seq(rnd.nextFloat().toString, "3.4028235e+38", "Infinity", "NaN", "1.25"))
  case 7 => pick(Seq("t", "f", "true", "FALSE", "T", "yes", "YES", "y", "n", "1", "0", "no", "on"))
  case 8 => pick(Seq(LocalDate.ofEpochDay(rnd.nextInt(3652059) - 719162).toString, "1970-01-01", "2000-02-29", "1900-03-01", "0001-01-01", "9999-12-31", "2026-02-30", "infinity"))
  case 9 | 10 => pick(Seq(ts(randomTs()), ts(randomTs()), "2026-01-01 10:00:00.", "2026-01-01 24:00:00", "infinity", "2026-01-01T10:00:00"))
  case 11 => pick(Seq(f"${rnd.nextInt(1000000)}%d.${rnd.nextInt(100)}%02d", f"-${rnd.nextInt(1000)}%d.${rnd.nextInt(100)}%02d", "0.00", "1.5", "1.005", "-0.50", "9999999999.99", "99999999999.99", "NaN"))
  case 12 => pick(Seq(f"${rnd.nextInt(1000000)}%d.${rnd.nextInt(1000)}%03d", "123456789012345678.5", "0.000000000000000001", "1", "-12.25", "1." + "1" * 19))
  case 13 => pick(Seq("12345", "99999", "100000", "-99999", "1.4", "1.5", "0"))
  case 14 => pick(Seq("{1,2,NULL}", "{}", "{-5}", "[1, 2]"))
  case 15 => pick(Seq(java.util.Base64.getMimeEncoder.encodeToString(Array.fill(rnd.nextInt(120))(rnd.nextInt(256).toByte)).replace("\r\n", "\\n"), ""))
}
def same(a: Any, b: Any): Boolean = (a, b) match {
  case (x: Array[Byte], y: Array[Byte]) => java.util.Arrays.equals(x, y)
  case _ => java.util.Objects.equals(a, b)
}
def outcome(f: => InternalRow): Either[String, Seq[Any]] = scala.util.Try(f).map(r => schema.fields.indices.map(i =>
  if (r.isNullAt(i)) null else r.get(i, schema.fields(i).dataType))).toEither.left.map(_.getClass.getSimpleName)
def compare(zone: String, rows: Int): String = {
  val util = SparkSchemaUtil(zone)
  val parser = new TextRowParser(schema, util)
  var differences = List.empty[String]
  var failures = 0
  for (_ <- 1 to rows) {
    val cut = rnd.nextInt(40) == 0 // a field too few or too many
    val fields = schema.fields.indices.map(field) ++ (if (cut && rnd.nextBoolean()) Seq("1") else Nil)
    val line = (if (cut && fields.size == schema.size) fields.init else fields).mkString("\t")
    val bytes = ("x" + line + "\n").getBytes(UTF_8) // the line inside a buffer, not at its start
    val expected = outcome(util.textToInternalRow(schema, line.split("\t", -1)))
    // as the reader: the line found in one pass with its fields, the next line after its LF
    val actual = outcome(parser.parseLine(bytes, 1))
    if (expected.isLeft) failures += 1
    val equal = (expected, actual) match {
      case (Right(e), Right(r)) => e.zip(r).forall { case (x, y) => same(x, y) }
      case (Left(_), Left(_)) => true
      case _ => false
    }
    if (!equal && differences.size < 5)
      differences ::= s"[$line] expected $expected, got $actual"
    if (actual.isRight && parser.nextLine != bytes.length && differences.size < 5)
      differences ::= s"[$line] the next line at ${parser.nextLine}, not ${bytes.length}"
  }
  if (differences.isEmpty) s"$rows rows, $failures failures, the same" else differences.mkString("; ").take(2000)
}
for (zone <- Seq("UTC", "Europe/Moscow", "America/New_York", "+05:30"))
  check(s"TextRowParser = textToInternalRow, DB time zone $zone", "50000 rows") { compare(zone, 50000).replaceAll(", \\d+ failures, the same$", "") }
done()
:quit

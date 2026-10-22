// Types: strings (non-ASCII, TEXT escapes, an empty string and "NULL" vs NULL), a date after 2038, timestamp
// microseconds; arrays of scalars (Cloudberry arrays), maps / structs / arrays of arrays and structs (JSONB) —
// written, the column types in Cloudberry, read back with the schema; existing int[] / text[] / jsonb columns
// read with the inferred schema and with a given one; arrays / structs into json / text columns; Cloudberry types
// Spark JDBC types its own way (bit, time, timetz) or text does not cast to in an INSERT (interval, inet, uuid[],
// ...), bytea[] — read and written back; smallint, timestamp_ntz written
:load /regression/probes/common.scala
// not types._: with Decimal in scope the REPL fails to print results (error reading Scala signature of Decimal.class)
import org.apache.spark.sql.types.{ArrayType, DateType, DecimalType, DoubleType, IntegerType, MapType, StringType, StructField, StructType, TimestampNTZType}
import org.apache.spark.sql.{DataFrame, Row}
import java.sql.DriverManager
val jdbc = DriverManager.getConnection(opts("url"), opts("user"), opts("password")); jdbc.setAutoCommit(true)
def exec(sql: String): Unit = { val st = jdbc.createStatement(); try st.execute(sql) finally st.close() }
def query(sql: String): String = { val st = jdbc.createStatement(); try { val rs = st.executeQuery(sql); val n = rs.getMetaData.getColumnCount; val out = scala.collection.mutable.ArrayBuffer[String](); while (rs.next()) out += (1 to n).map(c => String.valueOf(rs.getObject(c))).mkString("|"); out.mkString("; ") } finally st.close() }
def write(df: DataFrame, table: String, mode: String = "overwrite"): Unit = df.write.format("cloudberry").options(opts + ("dbtable" -> table)).mode(mode).save()
def read(table: String, schema: StructType = null): DataFrame = { val r = spark.read.format("cloudberry").options(opts + ("dbtable" -> table)); (if (schema != null) r.schema(schema) else r).load() }
def columnTypes(table: String): String = query(s"select string_agg(a.attname || ':' || t.typname, ' ' order by a.attnum) from pg_attribute a join pg_type t on t.oid = a.atttypid where a.attrelid = '$cbSchema.$table'::regclass and a.attnum > 0 and not a.attisdropped")
Seq("its_types_s", "its_types_t", "its_types_c", "its_types_g", "its_types_e", "its_types_r", "its_types_rc", "its_types_n").foreach(t => exec(s"drop table if exists $cbSchema.$t"))

// strings
val strings = Seq[(Int, String)]((1, "привет мир"), (2, "tab\there"), (3, "line\nbreak"), (4, "back\\slash"), (5, "quote\"d"), (6, ""), (7, "NULL"), (8, null), (9, "\\N"), (10, "cr\rhere"))
write(spark.createDataFrame(strings).toDF("id", "s"), "its_types_s")
check("strings: in Cloudberry as written", "1|10|1|1|1|1") { query(s"select (select count(*) from $cbSchema.its_types_s where s = 'привет мир'), (select length(s) from $cbSchema.its_types_s where id = 1), (select count(*) from $cbSchema.its_types_s where s = ''), (select count(*) from $cbSchema.its_types_s where s = 'NULL'), (select count(*) from $cbSchema.its_types_s where s is null), (select count(*) from $cbSchema.its_types_s where s = E'\\\\N')") }
check("strings: read back = written", true) { read("its_types_s").orderBy("id").collect().map(r => (r.getInt(0), r.getString(1))).toSeq == strings }
// server.compress: zstd frames from the segments — a compressible set (Cloudberry 2.1, as Greenplum 7.1, sends a zstd
// error code instead of a buffer zstd does not shrink, e.g. a few short rows: the read fails then)
val manyStrings = (1 to 2000).flatMap(k => strings.map { case (id, s) => (k * 100 + id, s) })
write(spark.createDataFrame(manyStrings).toDF("id", "s"), "its_types_s")
check("strings: read back with server.compress = written", true) {
  spark.read.format("cloudberry").options(opts + ("dbtable" -> "its_types_s") + ("server.compress" -> "true")).load()
    .orderBy("id").collect().map(r => (r.getInt(0), r.getString(1))).toSeq == manyStrings }

// a date after 2038, timestamp microseconds
val times = Seq((1, java.sql.Date.valueOf("2050-06-15"), java.sql.Timestamp.valueOf("2026-01-01 10:00:00.123456")))
write(spark.createDataFrame(times).toDF("id", "d", "ts"), "its_types_t")
check("date after 2038, timestamp microseconds: in Cloudberry", "2050-06-15|2026-01-01 10:00:00.123456") { query(s"select d::text, ts::text from $cbSchema.its_types_t") }
check("date after 2038, timestamp microseconds: read back = written", true) { read("its_types_t").collect().map(r => (r.getInt(0), r.getDate(1), r.getTimestamp(2))).toSeq == times }

// arrays, maps, structs written
val complexSchema = StructType(Seq(
  StructField("id", IntegerType),
  StructField("ints", ArrayType(IntegerType)),
  StructField("strs", ArrayType(StringType)),
  StructField("dates", ArrayType(DateType)),
  StructField("decs", ArrayType(DecimalType(10, 2))),
  StructField("nested", ArrayType(ArrayType(IntegerType))),
  StructField("st", StructType(Seq(StructField("a", IntegerType), StructField("b", StringType), StructField("c", ArrayType(StringType))))),
  StructField("m", MapType(StringType, IntegerType)),
  StructField("mi", MapType(IntegerType, StringType)),
  StructField("structs", ArrayType(StructType(Seq(StructField("k", StringType), StructField("v", DoubleType)))))))
val complexRows = Seq(
  Row(1, Seq(1, null, 3), Seq("a b", "c,d", "e\"f", "{g}", "h\\i", "привет", "", null, "NULL"), Seq(java.sql.Date.valueOf("2026-01-01"), null),
    Seq(new java.math.BigDecimal("1.50"), new java.math.BigDecimal("-2.25")), Seq(Seq(1, 2), Seq(3)), Row(1, "x\ty", Seq("p", null)),
    Map("k1" -> 1, "ключ" -> 2, "n" -> null), Map(1 -> "one", 2 -> null), Seq(Row("a", 1.5), Row("b", -0.25))),
  Row(2, Seq.empty[Int], Seq.empty[String], null, null, Seq.empty[Seq[Int]], null, Map.empty[String, Int], null, Seq.empty[Row]),
  Row(3, null, null, null, null, null, Row(null, null, null), null, null, null))
val complexDf = spark.createDataFrame(spark.sparkContext.parallelize(complexRows, 2), complexSchema)
// maps: Cloudberry jsonb keeps the keys sorted its own way — compare the entries sorted
def norm(df: DataFrame): String = df.selectExpr("id", "to_json(struct(ints, strs, dates, decs, nested, st, structs))",
  "to_json(array_sort(map_entries(m)))", "to_json(array_sort(map_entries(mi)))").orderBy("id").collect().map(_.mkString(" | ")).mkString("; ")
write(complexDf, "its_types_c")
check("complex: Cloudberry column types", "id:int4 ints:_int4 strs:_text dates:_date decs:_numeric nested:jsonb st:jsonb m:jsonb mi:jsonb structs:jsonb") { columnTypes("its_types_c") }
check("complex: a text[] in Cloudberry", """{"a b","c,d","e\"f","{g}","h\\i",привет,"",NULL,"NULL"}""") { query(s"select strs::text from $cbSchema.its_types_c where id = 1") }
check("complex: read back with the schema = written", norm(complexDf)) { norm(read("its_types_c", complexSchema)) }
check("complex: inferred schema (arrays, jsonb as text)", "ints:array<int> strs:array<string> dates:array<date> st:string") {
  read("its_types_c").schema.fields.filter(f => Set("ints", "strs", "dates", "st").contains(f.name)).map(f => s"${f.name}:${f.dataType.simpleString}").mkString(" ") }
check("complex: inferred schema, arrays read", "[1, null, 3] [a b, c,d, e\"f, {g}, h\\i, привет, , null, NULL]") {
  val r = read("its_types_c").where("id = 1").collect()(0); s"${r.getSeq[Any](r.fieldIndex("ints")).mkString("[", ", ", "]")} ${r.getSeq[Any](r.fieldIndex("strs")).mkString("[", ", ", "]")}" }
check("complex: jsonb read as text", """{"a": 1, "b": "x\ty", "c": ["p", null]}""") { read("its_types_c").where("id = 1").select("st").collect()(0).getString(0) }
check("complex: count", 3L) { read("its_types_c").count() }

// existing Cloudberry columns
exec(s"create table $cbSchema.its_types_g (id int, ia int[], ta text[], j jsonb) distributed by (id)")
exec(raw"""insert into $cbSchema.its_types_g values (1, '{1,2,NULL}', '{"a b",NULL,"c\"d"}', '{"x": 1, "y": [1, 2], "z": {"w": "привет"}}'), (2, '{}', '{}', null)""")
check("existing: inferred schema", "id:int ia:array<int> ta:array<string> j:string") { read("its_types_g").schema.fields.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(" ") }
check("existing: int[] / text[]", "1:[1, 2, null]:[a b, null, c\"d]; 2:[]:[]") {
  read("its_types_g").orderBy("id").collect().map(r => s"${r.getInt(0)}:${r.getSeq[Any](1).mkString("[", ", ", "]")}:${r.getSeq[Any](2).mkString("[", ", ", "]")}").mkString("; ") }
val gSchema = StructType(Seq(StructField("id", IntegerType), StructField("ia", ArrayType(IntegerType)), StructField("ta", ArrayType(StringType)),
  StructField("j", StructType(Seq(StructField("x", IntegerType), StructField("y", ArrayType(IntegerType)), StructField("z", StructType(Seq(StructField("w", StringType)))))))))
check("existing: jsonb read as a struct", "1 [1, 2] привет; null") {
  read("its_types_g", gSchema).orderBy("id").collect().map(r => Option(r.getStruct(3)).map(j => s"${j.getInt(0)} ${j.getSeq[Int](1).mkString("[", ", ", "]")} ${j.getStruct(2).getString(0)}").getOrElse("null")).mkString("; ") }
check("existing: jsonb read as a map (nested values as JSON)", "x=1 y=[1,2] z={\"w\":\"привет\"}") {
  val r = read("its_types_g", StructType(Seq(StructField("id", IntegerType), StructField("j", MapType(StringType, StringType))))).where("id = 1").collect()(0)
  r.getMap[String, String](1).toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(" ") }

// arrays / structs into json / text columns of an existing table
exec(s"create table $cbSchema.its_types_e (id int, ia jsonb, st text) distributed by (id)")
val eSchema = StructType(Seq(StructField("id", IntegerType), StructField("ia", ArrayType(IntegerType)), StructField("st", StructType(Seq(StructField("a", IntegerType))))))
write(spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, Seq(1, 2), Row(5))), 1), eSchema), "its_types_e", "append")
check("into existing json / text columns", "[1, 2]|{\"a\": 5}") { query(s"select ia::text, st from $cbSchema.its_types_e") }

// read, written back into a copy (LIKE), compared as text: before, bit / time / timetz were not read ("column is
// of type boolean but expression is of type bit"), interval / inet / money / point / uuid[] / jsonb[] / bytea[]
// were not written ("column is of type interval but expression is of type text")
def roundTrip(t: String, v: String): String = {
  exec(s"drop table if exists $cbSchema.its_types_r"); exec(s"drop table if exists $cbSchema.its_types_rc")
  exec(s"create table $cbSchema.its_types_r (id int, v $t) distributed by (id)")
  exec(s"insert into $cbSchema.its_types_r values (1, $v), (2, null)")
  exec(s"create table $cbSchema.its_types_rc (like $cbSchema.its_types_r) distributed by (id)")
  val df = read("its_types_r")
  write(df, "its_types_rc", "append")
  val diff = query(s"select count(*) from ((select id, v::text from $cbSchema.its_types_r except select id, v::text from $cbSchema.its_types_rc) " +
    s"union all (select id, v::text from $cbSchema.its_types_rc except select id, v::text from $cbSchema.its_types_r)) x")
  s"${df.schema("v").dataType.simpleString}, $diff different"
}
for ((t, v, sparkType) <- Seq(("bit(1)", "B'1'", "boolean"), ("bit(8)", "B'10101010'", "string"), ("time", "'10:20:30.123456'", "timestamp"),
  ("interval", "'1 day 02:03:04'", "string"), ("inet", "'10.0.0.1'", "string"), ("money", "12.34", "string"), ("point", "'(1,2)'", "string"),
  ("uuid[]", "'{123e4567-e89b-12d3-a456-426614174000}'", "array<string>"), ("jsonb[]", """ARRAY['{"a":1}'::jsonb]""", "array<string>"),
  ("bytea[]", """'{"\\x01ff",NULL}'""", "array<binary>")))
  check(s"$t: read, written back", s"$sparkType, 0 different") { roundTrip(t, v) }
check("timetz: read as the instant on 1970-01-01", "1970-01-01T07:20:30Z") {
  exec(s"drop table if exists $cbSchema.its_types_r"); exec(s"create table $cbSchema.its_types_r (id int, v timetz) distributed by (id)")
  exec(s"insert into $cbSchema.its_types_r values (1, '10:20:30+03')")
  read("its_types_r").collect()(0).getTimestamp(1).toInstant.toString }

// smallint / tinyint as smallint (was integer), timestamp_ntz as timestamp (was "Unsupported type")
write(spark.sql("select 1 as id, cast(1 as smallint) as s, cast(2 as tinyint) as b, timestamp_ntz'2026-01-01 10:20:30.123456' as ts"), "its_types_n")
check("smallint, tinyint, timestamp_ntz: written", "id:int4 s:int2 b:int2 ts:timestamp 2026-01-01 10:20:30.123456") {
  columnTypes("its_types_n") + " " + query(s"select ts::text from $cbSchema.its_types_n") }
check("timestamp_ntz: read with the schema, a pushed filter", "2026-01-01T10:20:30.123456, 1") {
  val df = read("its_types_n", StructType(Seq(StructField("id", IntegerType), StructField("ts", TimestampNTZType))))
  s"${df.collect()(0).get(1)}, ${df.where("ts = timestamp_ntz'2026-01-01 10:20:30.123456'").count()}" }

Seq("its_types_s", "its_types_t", "its_types_c", "its_types_g", "its_types_e", "its_types_r", "its_types_rc", "its_types_n").foreach(t => exec(s"drop table if exists $cbSchema.$t"))
jdbc.close()
done()
:quit

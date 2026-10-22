// A DataFrame built in Spark (not read from Cloudberry) in PROBE_PARTITIONS partitions: overwrite,
// overwrite + truncate, then append from 1 partition; a table created with distributedby / partitionclause /
// storageclause (PAX, AO column); the gpfdist ports of the tasks from a server.port range
:load /regression/probes/common.scala
val parts = sys.env.getOrElse("PROBE_PARTITIONS", "2").toInt
val src = cb("its_payments")
val local = spark.createDataFrame(spark.sparkContext.parallelize(src.collect().toSeq, 2), src.schema).localCheckpoint()
def write(df: org.apache.spark.sql.DataFrame, mode: String, extra: Map[String, String] = Map.empty): Unit = df.write.format("cloudberry").options(opts ++ extra + ("dbtable" -> "its_payments_copy")).mode(mode).save()
def copyRows: Long = cb("its_payments_copy").count()
check(s"$parts partitions: overwrite", 10L) { write(local.repartition(parts), "overwrite"); copyRows }
check(s"$parts partitions: overwrite + truncate", 10L) { write(local.repartition(parts), "overwrite", Map("truncate" -> "true")); copyRows }
check("1 partition: append", 20L) { write(local.coalesce(1), "append"); copyRows }
check("sum(payment_id) after append", 2L * (101 to 110).sum) { cb("its_payments_copy").selectExpr("sum(payment_id)").collect()(0).getLong(0) }
import java.sql.DriverManager
val jdbc = DriverManager.getConnection(opts("url"), opts("user"), opts("password")); jdbc.setAutoCommit(true)
def exec(sql: String): Unit = { val st = jdbc.createStatement(); try st.execute(sql) finally st.close() }
def text(sql: String): String = { val st = jdbc.createStatement(); try { val rs = st.executeQuery(sql); rs.next(); rs.getString(1) } finally st.close() }
def layout: String = text(s"select pg_get_table_distributedby('$cbSchema.its_payments_copy'::regclass) || ', ' || " +
  s"(select count(*) from pg_inherits where inhparent = '$cbSchema.its_payments_copy'::regclass) || ' leaves, ' || " +
  s"(select count(*) from $cbSchema.its_payments_copy) || ' rows'")
// both failed with a syntax error before: the clause of distributedby was appended as a pair, partitionclause in parentheses
val byMonth = "partition by range (payment_date) (start (date '2026-04-01') inclusive end (date '2026-06-01') exclusive every (interval '1 month'))"
check("a new table: distributedby, partitionclause", "DISTRIBUTED BY (client_id), 2 leaves, 10 rows") {
  exec(s"drop table if exists $cbSchema.its_payments_copy")
  write(local.repartition(parts), "append", Map("distributedby" -> "client_id", "partitionclause" -> byMonth)); layout }
check("overwrite (drop, create): distributedby replicated", "DISTRIBUTED REPLICATED, 0 leaves, 10 rows") {
  write(local.repartition(parts), "overwrite", Map("distributedby" -> "replicated")); layout }
// storageclause: the access method of a created table (before DISTRIBUTED BY), with partitionclause — of its leaves
def storage: String = text(s"select string_agg(distinct a.amname, ',') from pg_class c join pg_am a on a.oid = c.relam " +
  s"where c.oid = '$cbSchema.its_payments_copy'::regclass or c.oid in (select inhrelid from pg_inherits " +
  s"where inhparent = '$cbSchema.its_payments_copy'::regclass)") + ", " + layout
check("storageclause using pax: overwrite", "pax, DISTRIBUTED BY (payment_id), 0 leaves, 10 rows") {
  write(local.repartition(parts), "overwrite", Map("storageclause" -> "using pax", "distributedby" -> "payment_id")); storage }
check("storageclause using pax: append, read back", "20, 2110") {
  write(local.repartition(parts), "append"); cb("its_payments_copy").selectExpr("count(*)", "sum(payment_id)").collect()(0).toSeq.mkString(", ") }
check("storageclause ao_column + partitionclause", "ao_column, DISTRIBUTED BY (client_id), 2 leaves, 10 rows") {
  write(local.repartition(parts), "overwrite", Map("storageclause" -> "with (appendoptimized=true, orientation=column, compresstype=zstd)",
    "distributedby" -> "client_id", "partitionclause" -> byMonth)); storage }
// tasks of one JVM binding ports of a range at once: the second one used to fail in listen ("Address already in use")
check("server.port range: read", 10L) { cb("its_payments", Map("server.port" -> "46000-46100")).count() }
check("server.port range: overwrite", 10L) { write(local.repartition(parts), "overwrite", Map("server.port" -> "46000-46100")); copyRows }
jdbc.close()
done()
:quit

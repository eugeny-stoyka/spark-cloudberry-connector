// write.direct: a write of a DataFrame that is a plain read of Cloudberry goes without the Spark tasks — one
// INSERT ... SELECT in one session, gpfdist relays on the driver between two sessions (here: another user of the
// stand, under its own password). Checked: no Spark task, the rows, a column subset / rename / pushed filter,
// tables of other types), a failure of the source (the target keeps its rows), a DataFrame computed in Spark — through
// the Spark tasks
:load /regression/probes/common.scala
import org.apache.spark.scheduler._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import java.sql.DriverManager
val tasks = new java.util.concurrent.atomic.AtomicLong
spark.sparkContext.addSparkListener(new SparkListener { override def onTaskEnd(e: SparkListenerTaskEnd): Unit = tasks.incrementAndGet() })
def tasksOf(f: => Unit): Long = { Thread.sleep(1000); val before = tasks.get; f; Thread.sleep(2000); tasks.get - before }
val jdbc = DriverManager.getConnection(opts("url"), opts("user"), opts("password")); jdbc.setAutoCommit(true)
def exec(sql: String): Unit = { val st = jdbc.createStatement(); try st.execute(sql) finally st.close() }
def scalar(sql: String): String = { val st = jdbc.createStatement(); try { val rs = st.executeQuery(sql); rs.next(); rs.getString(1) } finally st.close() }
val direct = Map("write.direct" -> "true")
def write(df: DataFrame, table: String, mode: String, extra: Map[String, String] = Map.empty): Unit =
  df.write.format("cloudberry").options(opts ++ extra + ("dbtable" -> table)).mode(mode).save()
def sums(table: String, where: String = "true") =
  scalar(s"select count(*) || ' ' || coalesce(sum(id), 0) || ' ' || coalesce(sum(amount), 0) from $cbSchema.$table where $where")
def same(a: String, b: String): String = scalar(s"select (select count(*) from (select * from $cbSchema.$a except all select * from $cbSchema.$b) x) " +
  s"|| ' ' || (select count(*) from (select * from $cbSchema.$b except all select * from $cbSchema.$a) y) || ' ' || (select count(*) from $cbSchema.$a)")
def columnTypes(table: String): String = scalar(s"select string_agg(attname || ':' || atttypid::regtype, ' ' order by attnum) from pg_attribute " +
  s"where attrelid = '$cbSchema.$table'::regclass and attnum > 0 and not attisdropped")
val expected = sums("its_parts")

// one session: one INSERT ... SELECT inside Cloudberry
check("one session: no Spark task, the rows", s"tasks 0, $expected") {
  val n = tasksOf(write(cb("its_parts"), "its_direct", "overwrite", direct)); s"tasks $n, ${sums("its_direct")}" }
check("one session: append", "200000") { write(cb("its_parts"), "its_direct", "append", direct); scalar(s"select count(*) from $cbSchema.its_direct") }
check("one session: overwrite + truncate", "100000") {
  write(cb("its_parts"), "its_direct", "overwrite", direct + ("truncate" -> "true")); scalar(s"select count(*) from $cbSchema.its_direct") }
check("one session: a column subset, a rename, a pushed filter", s"tasks 0, k:bigint amount:numeric, ${scalar(s"select count(*) || ' ' || sum(id) from $cbSchema.its_parts where id <= 50000")}") {
  val n = tasksOf(write(cb("its_parts").where("id <= 50000").selectExpr("amount", "id as k").select("k", "amount"), "its_direct_k", "overwrite", direct))
  s"tasks $n, ${columnTypes("its_direct_k")}, ${scalar(s"select count(*) || ' ' || sum(k) from $cbSchema.its_direct_k")}" }

// the values: the same as through Spark, every type the stand reads and writes back
exec(s"drop table if exists $cbSchema.its_direct_src")
exec(s"create table $cbSchema.its_direct_src (id int, n numeric, t text, d date, ts timestamp, tz timestamptz, tm time, iv interval, " +
  "u uuid, b bytea, ia int[], ta text[], j jsonb, bo boolean, bt bit(1)) distributed by (id)")
exec(s"insert into $cbSchema.its_direct_src values (1, 1.5, E'tab\\there, line\\nbreak, back\\\\slash, привет', '2050-06-15', " +
  "'2026-01-01 10:00:00.123456', '2026-01-01 10:20:30+03', '10:20:30', '1 day 02:03:04', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', " +
  "E'\\\\x00ff10', '{1,NULL,3}', '{\"a b\",NULL,\"c,d\"}', '{\"k\": [1, \"x\"]}', true, B'1'), " +
  "(2, null, '', null, null, null, null, null, null, null, '{}', '{}', null, null, null), (3, 0.000001, 'NULL', '1900-03-01', " +
  "'1969-12-31 23:59:59.5', '1970-01-01 00:00:00+00', '23:59:59.999999', '-1 mon', null, '', null, '{\"\"}', '[]', false, B'0')")
def values(table: String, extra: Map[String, String]) = write(cb("its_direct_src"), table, "overwrite", extra)
check("values: new tables, direct = through Spark (rows differing both ways, rows)", "0 0 3") {
  values("its_direct_t1", direct); values("its_direct_t2", Map.empty); same("its_direct_t1", "its_direct_t2") }
check("values: new tables, the column types the same", true) { columnTypes("its_direct_t1") == columnTypes("its_direct_t2") }
for (t <- Seq("its_direct_e1", "its_direct_e2")) {
  exec(s"drop table if exists $cbSchema.$t")
  exec(s"create table $cbSchema.$t (id int, iv interval, u uuid, j jsonb, ia int[], tm time, bt bit(1)) distributed by (id)")
}
check("values: existing tables of other types, direct = through Spark", "0 0 3") {
  val df = cb("its_direct_src").select("id", "iv", "u", "j", "ia", "tm", "bt")
  write(df, "its_direct_e1", "append", direct); write(df, "its_direct_e2", "append"); same("its_direct_e1", "its_direct_e2") }

// two sessions: another user under its own password — gpfdist relays on the driver
exec("drop role if exists its_relay")
exec("create role its_relay login password 'relay'")
exec("alter role its_relay with createexttable (type = 'readable', protocol = 'gpfdist')")
exec("alter role its_relay with createexttable (type = 'writable', protocol = 'gpfdist')")
exec(s"grant usage on schema $cbSchema to its_relay")
exec(s"grant select on $cbSchema.its_parts, $cbSchema.its_direct_src to its_relay")
val relay = opts ++ Map("user" -> "its_relay", "password" -> "relay")
def relayRead(table: String) = spark.read.format("cloudberry").options(relay + ("dbtable" -> table)).load()
check("relay: another user, no Spark task, the rows", s"tasks 0, $expected") {
  val n = tasksOf(write(relayRead("its_parts"), "its_direct", "overwrite", direct)); s"tasks $n, ${sums("its_direct")}" }
check("relay: a column subset, a rename, a pushed filter", scalar(s"select count(*) || ' ' || sum(id) from $cbSchema.its_parts where id <= 50000")) {
  write(relayRead("its_parts").where("id <= 50000").selectExpr("id as k", "amount"), "its_direct_k", "overwrite", direct)
  scalar(s"select count(*) || ' ' || sum(k) from $cbSchema.its_direct_k") }
check("relay: values, direct = through Spark", "0 0 3") { write(relayRead("its_direct_src"), "its_direct_t1", "overwrite", direct); same("its_direct_t1", "its_direct_t2") }
check("relay: existing tables of other types, direct = through Spark", "0 0 3") {
  exec(s"truncate $cbSchema.its_direct_e1")
  write(relayRead("its_direct_src").select("id", "iv", "u", "j", "ia", "tm", "bt"), "its_direct_e1", "append", direct)
  same("its_direct_e1", "its_direct_e2") }
// a failure of the source during the transfer (a division by zero on the segments): the write fails, the target
// keeps its rows — through the relays (the INSERT of the target is cancelled) and in one session
val failing = s"select id, d, (amount / (id - id))::numeric(12,2) as amount from $cbSchema.its_parts"
check("relay: the source fails during the transfer", "division by zero, the target keeps its rows") {
  val e = scala.util.Try(write(relayRead(failing), "its_direct", "append", direct)).failed.map(_.getMessage).getOrElse("no failure")
  (if (e.contains("division by zero")) "division by zero" else e.take(200)) +
    (if (sums("its_direct") == expected) ", the target keeps its rows" else s", the target has ${sums("its_direct")}") }
check("one session: the source fails", "division by zero, the target keeps its rows") {
  val e = scala.util.Try(write(cb(failing), "its_direct", "append", direct)).failed.map(_.getMessage).getOrElse("no failure")
  (if (e.contains("division by zero")) "division by zero" else e.take(200)) +
    (if (sums("its_direct") == expected) ", the target keeps its rows" else s", the target has ${sums("its_direct")}") }

// not a plain read: through the Spark tasks
check("a column computed in Spark: through the Spark tasks", true) {
  val n = tasksOf(write(cb("its_parts").withColumn("id", col("id") + 1), "its_direct", "overwrite", direct))
  n > 0 && sums("its_direct") == scalar(s"select count(*) || ' ' || (sum(id) + count(*)) || ' ' || sum(amount) from $cbSchema.its_parts") }

for (t <- Seq("its_direct", "its_direct_k", "its_direct_src", "its_direct_t1", "its_direct_t2", "its_direct_e1", "its_direct_e2"))
  exec(s"drop table if exists $cbSchema.$t")
exec("drop owned by its_relay"); exec("drop role its_relay")
jdbc.close()
done()
:quit

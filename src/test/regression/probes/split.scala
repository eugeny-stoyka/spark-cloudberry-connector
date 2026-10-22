// read.split: every Spark partition is its own transfer of a part — leaf partitions (run.sh creates its_parts:
// 100000 rows in 7 monthly leaves, the July one empty), segments, ranges of a column; a failed task reads only its
// part again (local[2,2] — one retry); the parts share a snapshot. The classic read gives the expected values.
:load /regression/probes/common.scala
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
val split = Map("read.split" -> "partitions")
def sums(df: DataFrame): String = df.selectExpr("count(*)", "sum(id)", "sum(amount)").collect()(0).toSeq.mkString(" ")
val expected = sums(cb("its_parts"))
val may = "d >= date '2026-05-01' and d < date '2026-06-01'"
check("classic read: count sum(id)", "100000 5000050000") { expected.split(" ").take(2).mkString(" ") }
check("split: partitions (7 leaves, 4 x 2 slots)", 7) { cb("its_parts", split).rdd.getNumPartitions }
check("split: the same rows", expected) { sums(cb("its_parts", split)) }
check("split, read.split.parts=3: partitions", 3) { cb("its_parts", split + ("read.split.parts" -> "3")).rdd.getNumPartitions }
check("split, read.split.parts=3: the same rows", expected) { sums(cb("its_parts", split + ("read.split.parts" -> "3"))) }
check("split, a pushed filter (May): the same rows", cb("its_parts").where(may).count()) { cb("its_parts", split).where(may).count() }
check("split, limit 5", 5L) { cb("its_parts", split).limit(5).count() }
// a task fails once after 10 of its rows: Spark runs it again, the new attempt reads its part again
def failingOnce(df: DataFrame): Long = df.rdd.mapPartitions { it =>
  val ctx = TaskContext.get()
  if (ctx.partitionId() == 0 && ctx.attemptNumber() == 0) {
    it.take(10).foreach(_ => ())
    throw new RuntimeException("injected task failure")
  }
  it
}.count()
check("split: a task fails once, its part is read again: rows", 100000L) { failingOnce(cb("its_parts", split)) }
check("split: not applied to a query (read as one transfer): rows", 100000L) {
  cb(s"select * from $cbSchema.its_parts", split).count()
}

// read.split=segments: a part per primary segment (gp_segment_id = N goes to that segment only, EXPLAIN checked)
val segments = Map("read.split" -> "segments")
check("segments: partitions (= primary segments)", cbSegments) { cb("its_parts", segments).rdd.getNumPartitions }
check("segments: the same rows", expected) { sums(cb("its_parts", segments)) }
check("segments: a table without partitions: rows, sum", "10 1055") {
  cb("its_payments", segments).selectExpr("count(*)", "sum(payment_id)").collect()(0).toSeq.mkString(" ") }
check("segments: a task fails once, its part is read again: rows", 100000L) { failingOnce(cb("its_parts", segments)) }

// read.split=column: ranges of a column, as partitionColumn of Spark JDBC
val byId = Map("read.split" -> "column", "read.split.column" -> "id", "read.split.lowerBound" -> "1",
  "read.split.upperBound" -> "100001", "read.split.parts" -> "5")
check("column: partitions", 5) { cb("its_parts", byId).rdd.getNumPartitions }
check("column: the same rows", expected) { sums(cb("its_parts", byId)) }
check("column, a date (the partition key): the same rows", expected) {
  sums(cb("its_parts", Map("read.split" -> "column", "read.split.column" -> "d", "read.split.lowerBound" -> "2026-01-01",
    "read.split.upperBound" -> "2026-08-01"))) }
check("column on a query: rows", 100000L) { cb(s"select * from $cbSchema.its_parts", byId).count() }

// read.split.snapshot: rows inserted while the read runs (by the first task, at its first row) are not seen by the
// parts that start after it — all the parts read one snapshot; without it the later parts see them
import java.sql.DriverManager
val jdbc = DriverManager.getConnection(opts("url"), opts("user"), opts("password")); jdbc.setAutoCommit(true)
def exec(sql: String): Unit = { val st = jdbc.createStatement(); try st.execute(sql) finally st.close() }
def insertDuringRead(df: DataFrame): Long = {
  val (url, user, password, table) = (opts("url"), opts("user"), opts("password"), s"$cbSchema.its_parts")
  df.rdd.mapPartitions { it =>
    val ctx = TaskContext.get()
    if (ctx.partitionId() == 0 && ctx.attemptNumber() == 0 && it.hasNext) {
      val c = java.sql.DriverManager.getConnection(url, user, password)
      try c.createStatement().execute(s"insert into $table select -m, (date '2026-01-15' + (m - 1) * interval '1 month')::date, 0 from generate_series(1, 7) m")
      finally c.close()
    }
    it
  }.count()
}
check("snapshot: rows inserted during the read are not seen: rows", 100000L) { insertDuringRead(cb("its_parts", split)) }
exec(s"delete from $cbSchema.its_parts where id < 0")
check("no snapshot (read.split.snapshot=false): the later parts see them", true) {
  insertDuringRead(cb("its_parts", split + ("read.split.snapshot" -> "false"))) > 100000L }
exec(s"delete from $cbSchema.its_parts where id < 0")
jdbc.close()
done()
:quit

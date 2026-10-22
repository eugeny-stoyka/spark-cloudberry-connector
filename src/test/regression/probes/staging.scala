// write.staging=true: every write task attempt fills a staging table of its own, the job commit moves them into the
// target in one transaction (an overwrite with truncate — truncated in it); a write task failing once is retried —
// the rows are complete, no duplicates, no staging table left (local[2,2] — one retry)
:load /regression/probes/common.scala
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
import java.sql.DriverManager
val jdbc = DriverManager.getConnection(opts("url"), opts("user"), opts("password")); jdbc.setAutoCommit(true)
def scalar(sql: String): Long = { val st = jdbc.createStatement(); try { val rs = st.executeQuery(sql); rs.next(); rs.getLong(1) } finally st.close() }
val staging = opts + ("write.staging" -> "true") + ("dbtable" -> "its_payments_copy")
def sums(): String = cb("its_payments_copy").selectExpr("count(*)", "count(distinct id)", "sum(id)").collect()(0).toSeq.mkString(" ")
def stagingTablesLeft(): Long = scalar("select count(*) from pg_tables where tablename like 'scc_stg_%'")
val src = spark.range(0, 100000, 1, 4).toDF("id").selectExpr("id", "id % 7 as k")
check("staging, overwrite: rows, distinct, sum", "100000 100000 4999950000") { src.write.format("cloudberry").options(staging).mode("overwrite").save(); sums() }
check("staging, append: rows, distinct, sum", "200000 100000 9999900000") { src.write.format("cloudberry").options(staging).mode("append").save(); sums() }
check("staging, overwrite + truncate (in the commit): rows, distinct, sum", "100000 100000 4999950000") {
  src.write.format("cloudberry").options(staging + ("truncate" -> "true")).mode("overwrite").save(); sums() }
// a write task failing once after 1000 rows; write.repartition=false — the failure is in the write task itself
def failingOnce(df: DataFrame): DataFrame = spark.createDataFrame(df.rdd.mapPartitions { it =>
  val ctx = TaskContext.get()
  if (ctx.partitionId() == 0 && ctx.attemptNumber() == 0)
    it.zipWithIndex.map { case (row, i) => if (i == 1000) throw new RuntimeException("injected task failure"); row }
  else it
}, df.schema)
check("staging, a write task fails once, retried: rows, distinct, sum", "100000 100000 4999950000") {
  failingOnce(src).write.format("cloudberry").options(staging + ("write.repartition" -> "false")).mode("overwrite").save(); sums() }
check("staging: no staging table left", 0L) { stagingTablesLeft() }
jdbc.close()
done()
:quit

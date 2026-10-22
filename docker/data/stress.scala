// The measurements of the stress test on its_stress (stress.sql), three scenarios:
//   export  Spark reads its_stress from Cloudberry and writes it into HDFS as Parquet (STRESS_HDFS_PATH), in every
//           read mode of the connector
//   import  Spark reads that Parquet from HDFS and writes it into its_stress_copy in Cloudberry (overwrite)
//   copy    Spark reads its_stress from Cloudberry and writes it into its_stress_copy (overwrite)
// The time is of the transfer only; the sink is checked after it, untimed (its rows, the sums for the Parquet). A
// line per step: RESULT <step> <seconds> s <the check> <MB/s by the text size of the table>, also in $RESULTS_FILE.
//   docker/run.sh stress run           (YARN, the executors of STAND_RESOURCES)
// STRESS_STEPS — the steps to run, by a part of their names (comma separated), default all; an import needs the
// Parquet of an export (the last export of the run — the classic one — or kept by STRESS_KEEP_PARQUET=1).
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.sql.DriverManager
import org.apache.hadoop.fs.Path
val opts = Map("url" -> "jdbc:postgresql://cloudberry:5432/cloudberry", "user" -> "gpadmin", "password" -> "gpadmin",
  "dbschema" -> "test")
val parquet = sys.env.getOrElse("STRESS_HDFS_PATH", "/stress/its_stress.parquet")
def cb(table: String, extra: Map[String, String] = Map.empty) =
  spark.read.format("cloudberry").options(opts ++ extra + ("dbtable" -> table)).load()
val jdbc = DriverManager.getConnection(opts("url"), opts("user"), opts("password"))
val textBytes = { val rs = jdbc.createStatement().executeQuery(
  "select obj_description('test.its_stress'::regclass)"); rs.next(); rs.getString(1).toDouble }
jdbc.close()
def report(line: String): Unit = {
  println(s"RESULT $line")
  sys.env.get("RESULTS_FILE").foreach(f => Files.write(Paths.get(f), (line + "\n").getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND))
}
val steps = sys.env.getOrElse("STRESS_STEPS", "").split(",").map(_.trim).filter(_.nonEmpty)
/** a step: `transfer` timed, `check` after it, untimed — its result goes into the line */
def timed(step: String)(transfer: => Unit)(check: => Any): Unit = if (steps.isEmpty || steps.exists(step.contains)) {
  val t0 = System.nanoTime()
  val r = scala.util.Try(transfer)
  val s = (System.nanoTime() - t0) / 1e9
  val result = r.map(_ => scala.util.Try(check).map(String.valueOf).getOrElse("check FAILED")).getOrElse("FAILED " + r.failed.get.getMessage.take(200))
  report(f"$step%-44s ${s}%7.1f s  $result  ${textBytes / 1e6 / s}%7.1f MB/s")
}
report(f"its_stress: ${textBytes / 1e9}%.1f GB of text")
val fs = new Path(parquet).getFileSystem(spark.sparkContext.hadoopConfiguration)

// a warm-up, untimed: 2 million rows exported and imported — the first step does not pay for the start of the
// executors, the JIT, the classes of the connector and of Parquet
cb("select * from test.its_stress where id <= 2000000").write.mode("overwrite").parquet(parquet + ".warmup")
spark.read.parquet(parquet + ".warmup").write.format("cloudberry").options(opts + ("dbtable" -> "its_stress_copy")).mode("overwrite").save()
fs.delete(new Path(parquet + ".warmup"), true)

// export: every column of the Parquet checked — the count and the sums, the same for every read mode
def parquetCheck: String = {
  val c = fs.getContentSummary(new Path(parquet))
  val sums = spark.read.parquet(parquet).selectExpr("count(*)", "sum(id)", "sum(amount)", "sum(length(note))", "count(distinct d)").collect()(0).mkString(" ")
  f"$sums; ${c.getLength / 1e9}%.2f GB Parquet, ${c.getFileCount - 1} files"
}
def export(extra: Map[String, String]): Unit = cb("its_stress", extra).write.mode("overwrite").parquet(parquet)
timed("export, read.split=partitions")(export(Map("read.split" -> "partitions")))(parquetCheck)
timed("export, read.split=partitions, server.compress")(export(Map("read.split" -> "partitions", "server.compress" -> "true")))(parquetCheck)
timed("export, read.split=segments")(export(Map("read.split" -> "segments")))(parquetCheck)
timed("export, server.compress")(export(Map("server.compress" -> "true")))(parquetCheck)
timed("export, classic")(export(Map.empty))(parquetCheck) // the last: its Parquet is the source of the imports

// import and copy: the rows of its_stress_copy
def copyRows: String = s"${cb("its_stress_copy").count()} rows in its_stress_copy"
def write(df: => org.apache.spark.sql.DataFrame, extra: Map[String, String] = Map.empty): Unit =
  df.write.format("cloudberry").options(opts ++ extra + ("dbtable" -> "its_stress_copy")).mode("overwrite").save()
timed("import, write.repartition=true")(write(spark.read.parquet(parquet)))(copyRows)
timed("import, write.repartition=false")(write(spark.read.parquet(parquet), Map("write.repartition" -> "false")))(copyRows)
timed("import, write.staging")(write(spark.read.parquet(parquet), Map("write.staging" -> "true")))(copyRows)
timed("copy, classic")(write(cb("its_stress")))(copyRows)
timed("copy, write.staging")(write(cb("its_stress"), Map("write.staging" -> "true")))(copyRows)
if (!sys.env.get("STRESS_KEEP_PARQUET").contains("1"))
  fs.delete(new Path(parquet), true)
report("DONE")
:quit

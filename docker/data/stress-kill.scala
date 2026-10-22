// An export of its_stress into HDFS (Parquet, as the export of stress.scala) during which an executor process is
// killed (kill -9 from the host, STRESS_KILL_AFTER seconds after "RESULT start"; docker/run.sh stress run):
// MODE=split — read.split=partitions, the parts of its tasks are read and written again on the other executors,
// the Parquet is checked (the count and the sums); MODE=classic — one transfer for the whole read, expected to fail.
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}
import org.apache.hadoop.fs.Path
val opts = Map("url" -> "jdbc:postgresql://cloudberry:5432/cloudberry", "user" -> "gpadmin", "password" -> "gpadmin",
  "dbschema" -> "test")
val mode = sys.env("MODE")
val parquet = sys.env.getOrElse("STRESS_HDFS_PATH", "/stress/its_stress.parquet") + ".kill"
val extra = if (mode == "split") Map("read.split" -> "partitions") else Map.empty[String, String]
val df = spark.read.format("cloudberry").options(opts ++ extra + ("dbtable" -> "its_stress")).load()
df.schema.size
println("RESULT start")
val t0 = System.nanoTime()
val written = scala.util.Try(df.write.mode("overwrite").parquet(parquet))
val seconds = (System.nanoTime() - t0) / 1e9
val outcome = written.map(_ => spark.read.parquet(parquet).selectExpr("count(*)", "sum(id)", "sum(amount)", "sum(length(note))")
  .collect()(0).mkString(" ")).recover { case e => "FAILED " + String.valueOf(e.getMessage).take(200) }.get
val line = f"export, an executor killed, $mode%-8s $seconds%7.1f s  $outcome"
println(s"RESULT $line")
sys.env.get("RESULTS_FILE").foreach(f => Files.write(Paths.get(f), (line + "\n").getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND))
val path = new Path(parquet)
path.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(path, true)
:quit

// Common definitions of the regression probes: every probe starts with :load of this file.
// A check writes one line to $RESULTS_FILE (and to stdout): <step> -> <value> [<ms> ms]; a value other
// than expected is marked MISMATCH, an exception - FAILED. The last line of a probe is DONE: a probe
// that stopped half-way (a compile error of a REPL line, a hang) is visible to run.sh.
import scala.util.{Failure, Success, Try}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}
val cbSchema = sys.env.getOrElse("CB_SCHEMA", "test")
val opts = Map(
  "url" -> sys.env.getOrElse("CB_URL", "jdbc:postgresql://cloudberry:5432/cloudberry"),
  "user" -> sys.env.getOrElse("CB_USER", "gpadmin"),
  "password" -> sys.env.getOrElse("CB_PASSWORD", "gpadmin"),
  "dbschema" -> cbSchema)
def report(line: String): Unit = {
  println(s"RESULT $line")
  sys.env.get("RESULTS_FILE").foreach(f => Files.write(Paths.get(f), (line + "\n").getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND))
}
def check(step: String, expected: Any = null)(f: => Any): Unit = {
  val t0 = System.currentTimeMillis()
  val result = Try(f) match {
    case Success(v) if expected == null || v == expected => String.valueOf(v)
    case Success(v) => s"$v MISMATCH (expected $expected)"
    case Failure(e) => s"FAILED ${e.getClass.getName}: ${String.valueOf(e.getMessage).take(300)}"
  }
  report(s"$step -> $result [${System.currentTimeMillis() - t0} ms]")
}
def checkFails(step: String, message: String)(f: => Any): Unit = {
  val result = Try(f) match {
    case Failure(e) if String.valueOf(e.getMessage).contains(message) => s"failed as expected: ${String.valueOf(e.getMessage).take(200)}"
    case Failure(e) => s"FAILED ${e.getClass.getName}: ${String.valueOf(e.getMessage).take(300)} MISMATCH (expected: $message)"
    case Success(v) => s"$v MISMATCH (expected a failure: $message)"
  }
  report(s"$step -> $result")
}
def done(): Unit = report("DONE")
def cb(table: String, extra: Map[String, String] = Map.empty) = spark.read.format("cloudberry").options(opts ++ extra + ("dbtable" -> table)).load()
// the primary segments of the stand (run.sh queries them) and the task slots of the run: a classic transfer has
// min(segments, task slots) tasks
val cbSegments = sys.env.getOrElse("CB_SEGMENTS", "4").toInt
def taskSlots: Int = {
  val local = """local\[(\d+)(,\d+)?\]""".r
  spark.sparkContext.master match {
    case local(n, _) => n.toInt
    case _ => spark.conf.get("spark.executor.instances").toInt * spark.conf.get("spark.executor.cores").toInt
  }
}

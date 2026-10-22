// spark.speculation=true (run.sh passes it, YARN — Spark does not speculate in local mode): a read and a write
// refuse to start — a speculative copy of a transfer task would abort the transfer midway; a split read
// (read.split) is allowed — a copy of its task is a transfer of its own
:load /regression/probes/common.scala
check("spark.speculation", "true") { spark.conf.get("spark.speculation") }
checkFails("read is refused", "spark.speculation=true") { cb("its_payments").count() }
check("read.split=partitions is allowed: rows", 100000L) { cb("its_parts", Map("read.split" -> "partitions")).count() }
// the rows from Spark and counted over JDBC: a classic read is refused here
check("write.staging is allowed: rows", 1000L) {
  spark.range(1000).toDF("id").write.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy") + ("write.staging" -> "true")).mode("overwrite").save()
  val c = java.sql.DriverManager.getConnection(opts("url"), opts("user"), opts("password"))
  try { val rs = c.createStatement().executeQuery(s"select count(*) from $cbSchema.its_payments_copy"); rs.next(); rs.getLong(1) } finally c.close() }
checkFails("write is refused", "spark.speculation=true") {
  spark.range(10).toDF("id").write.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy")).mode("overwrite").save()
}
done()
:quit

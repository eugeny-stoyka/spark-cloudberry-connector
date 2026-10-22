// Backpressure under load: buffer.max=2m, write of 3 million rows from 4 partitions and read back
// (count, sum, distinct) - the bounded buffers must not lose or duplicate rows
:load /regression/probes/common.scala
val n = 3000000L
val small = Map("buffer.max" -> "2m")
check("write 3M rows, buffer.max=2m") { spark.range(0, n, 1, 4).selectExpr("id", "repeat('x', 100) as payload").write.format("cloudberry").options(opts ++ small + ("dbtable" -> "its_big")).mode("overwrite").save(); "ok" }
check("read back, buffer.max=2m: rows, sum(id), distinct", s"$n, ${n * (n - 1) / 2}, $n") { val r = cb("its_big", small).selectExpr("count(*)", "sum(id)", "count(distinct id)").collect()(0); s"${r(0)}, ${r(1)}, ${r(2)}" }
// server.compress: the segments send zstd frames, the tasks unpack them
check("read back, server.compress=true: rows, sum(id), distinct, payload", s"$n, ${n * (n - 1) / 2}, $n, 100") {
  val r = cb("its_big", small + ("server.compress" -> "true")).selectExpr("count(*)", "sum(id)", "count(distinct id)", "max(length(payload))").collect()(0)
  s"${r(0)}, ${r(1)}, ${r(2)}, ${r(3)}" }
done()
:quit

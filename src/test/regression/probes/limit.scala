// Early close of the readers (limit) on 1 million rows, then a full count and sum of the same table:
// the cancelled transfer must not break the next ones
:load /regression/probes/common.scala
val n = 1000000L
check("write 1M rows") { spark.range(0, n, 1, 4).selectExpr("id", "repeat('y', 50) as payload").write.format("cloudberry").options(opts + ("dbtable" -> "its_big")).mode("overwrite").save(); "ok" }
def big = cb("its_big")
check("limit 5", 5) { big.limit(5).collect().length }
check("full count after limit", n) { big.count() }
check("sum(id)", n * (n - 1) / 2) { big.selectExpr("sum(id)").collect()(0).getLong(0) }
done()
:quit

// Statistics of a read for the optimizer: the catalog rows / bytes of a table (run.sh analyzes its_payments),
// unknown for a query — Spark then takes spark.sql.defaultSizeInBytes and never broadcasts it (before: 2000
// bytes / 20 rows for any source, a large table in a join was broadcast)
:load /regression/probes/common.scala
def stats(df: org.apache.spark.sql.DataFrame) = df.queryExecution.optimizedPlan.stats
val defaultSize = spark.sessionState.conf.defaultSizeInBytes
check("table: rows", Some(10L)) { stats(cb("its_payments")).rowCount.map(_.toLong) }
check("table: bytes known (not 2000, not the default)", true) { val b = stats(cb("its_payments")).sizeInBytes; b > 2000 && b < defaultSize }
check("query: rows unknown", None) { stats(cb(s"select * from $cbSchema.its_payments")).rowCount }
check("query: bytes = spark.sql.defaultSizeInBytes", true) { stats(cb(s"select * from $cbSchema.its_payments")).sizeInBytes == defaultSize }
check("table read still works: rows", 10L) { cb("its_payments").count() }
// a partitioned table (run.sh analyzes its_parts): the rows of the leaves — ANALYZE of the root puts all of them
// into its reltuples too (PostgreSQL 14), they were counted twice
check("partitioned table: rows (the leaves only)", Some(100000L)) { stats(cb("its_parts")).rowCount.map(_.toLong) }
done()
:quit

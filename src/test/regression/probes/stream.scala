// Structured Streaming: micro-batch read from Cloudberry (offset.select + sqlTransfer) of the rows inserted
// after the start, and write of a rate stream into Cloudberry
:load /regression/probes/common.scala
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import java.sql.DriverManager
// stop between micro-batches: stop() during a batch cancels its job, the write of that epoch is aborted
// (rolled back) and Spark logs it as ERROR. The next trigger is up to 2 s away, so the gap is safe
def stopBetweenBatches(q: StreamingQuery): Unit = { while (q.status.isTriggerActive) Thread.sleep(20); q.stop() }
val jdbc = DriverManager.getConnection(opts("url"), opts("user"), opts("password")); jdbc.setAutoCommit(true)
def exec(sql: String): Unit = { val st = jdbc.createStatement(); try st.execute(sql) finally st.close() }
def scalar(sql: String): Long = { val st = jdbc.createStatement(); try { val rs = st.executeQuery(sql); rs.next(); rs.getLong(1) } finally st.close() }
val src = s"$cbSchema.its_stream_src"
val sink = s"$cbSchema.its_stream_sink"
exec(s"drop table if exists $src"); exec(s"drop table if exists $sink")
exec(s"create table $src (id bigint, ts timestamp) distributed by (id)")
exec(s"insert into $src select g, now() from generate_series(1, 1000) g")
val offSel = s"select json_build_object('max_id', coalesce(max(id), 0))::text from $src"
val sqlT = s"insert into <ext_table> select id, ts from $src where id > coalesce((nullif('<start_offset_json>', '[null]')::json->>'max_id')::bigint, 0) and id <= ('<end_offset_json>'::json->>'max_id')::bigint"
val rq = spark.readStream.format("cloudberry").options(opts + ("dbtable" -> "its_stream_src") + ("sqlTransfer" -> sqlT) + ("offset.select" -> offSel)).load().writeStream.format("memory").queryName("cb_in").trigger(Trigger.ProcessingTime("2 seconds")).start()
Thread.sleep(8000); exec(s"insert into $src select g, now() from generate_series(1001, 3000) g")
Thread.sleep(8000); exec(s"insert into $src select g, now() from generate_series(3001, 4000) g")
Thread.sleep(10000); stopBetweenBatches(rq)
check("streaming read: query error", None) { rq.exception.map(_.getMessage.take(200)) }
check("streaming read: rows, distinct, min..max", "3000, 3000, 1001..4000") { val r = spark.sql("select count(*), count(distinct id), min(id), max(id) from cb_in").collect()(0); s"${r(0)}, ${r(1)}, ${r(2)}..${r(3)}" }
val wq = spark.readStream.format("rate").option("rowsPerSecond", 500).load().selectExpr("value as id", "timestamp as ts").writeStream.format("cloudberry").options(opts + ("dbtable" -> "its_stream_sink")).option("checkpointLocation", s"/tmp/chk-${System.currentTimeMillis()}").outputMode("append").trigger(Trigger.ProcessingTime("2 seconds")).start()
Thread.sleep(15000); stopBetweenBatches(wq)
val produced = wq.recentProgress.map(_.numInputRows).sum
val written = scalar(s"select count(*) from $sink")
val distinct = scalar(s"select count(distinct id) from $sink")
check("streaming write: query error", None) { wq.exception.map(_.getMessage.take(200)) }
check("streaming write: rows in Cloudberry, distinct, by Spark batches") { s"$written, $distinct, $produced" }
// stop() may interrupt a batch already committed in Cloudberry: then Cloudberry has more rows than Spark counted
check("streaming write: rows written, no duplicates, not less than Spark committed", true) { produced > 0 && distinct == written && written >= produced }
exec(s"drop table if exists $src"); exec(s"drop table if exists $sink"); jdbc.close()
done()
:quit

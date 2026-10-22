// The data source name cloudberry: read, overwrite with truncate, read back, reads without the distribution key;
// option names in any case
:load /regression/probes/common.scala
check("cloudberry: read its_payments", 10L) { spark.read.format("cloudberry").options(opts + ("dbtable" -> "its_payments")).load().count() }
check("cloudberry: overwrite + truncate its_payments_copy, read back", 10L) {
  spark.read.format("cloudberry").options(opts + ("dbtable" -> "its_payments")).load().localCheckpoint()
    .write.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy") + ("truncate" -> "true")).mode("overwrite").save()
  spark.read.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy")).load().count()
}
// its_payments is distributed by payment_id: a read without it used to fail (DISTRIBUTED BY of a column not read)
check("a read without the distribution key column", 10) { spark.read.format("cloudberry").options(opts + ("dbtable" -> "its_payments")).load().select("note").collect().length }
check("a pushed filter on the distribution key, other columns read", "payment 105") {
  spark.read.format("cloudberry").options(opts + ("dbtable" -> "its_payments")).load().where("payment_id = 105").select("note").collect()(0).getString(0) }
// option names in any case: sqltransfer (so documented) and SQLTRANSFER were ignored, only sqlTransfer was taken
check("sqltransfer in lower case: read", 3) {
  cb("select payment_id, note from its_payments where false",
    Map("sqltransfer" -> s"insert into <ext_table> select payment_id, note from $cbSchema.its_payments where payment_id < 104")).collect().length }
check("SQLTRANSFER in upper case: write", 14L) {
  cb("its_payments").write.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy") +
    ("SQLTRANSFER" -> s"insert into $cbSchema.its_payments_copy select * from <ext_table> where payment_id > 106")).mode("append").save()
  cb("its_payments_copy").count() }
check("transfer.settlems in lower case: taken", "transfer.settleMs failed") {
  Try(cb("its_payments", Map("transfer.settlems" -> "x")).count()).map(_ => "ignored").getOrElse("transfer.settleMs failed") }
done()
:quit

// A DataFrame read from Cloudberry written straight back into Cloudberry (a read nested in a write):
// overwrite without and with truncate, the copy equals the source
:load /regression/probes/common.scala
def src = cb("its_payments")
def copy = cb("its_payments_copy")
def diff(): Long = src.exceptAll(copy).count() + copy.exceptAll(src).count()
check("read its_payments", 10L) { src.count() }
check("columns", "payment_id,client_id,amount,payment_date,created_at,note") { src.columns.mkString(",") }
check("overwrite its_payments_copy: rows", 10L) { src.write.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy")).mode("overwrite").save(); copy.count() }
check("overwrite: rows differing from the source", 0L) { diff() }
check("overwrite + truncate: rows", 10L) { src.write.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy") + ("truncate" -> "true")).mode("overwrite").save(); copy.count() }
check("overwrite + truncate: rows differing from the source", 0L) { diff() }
done()
:quit

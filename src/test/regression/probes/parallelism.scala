// The parallelism option: read partitions, number of write tasks (SparkListener), an invalid value.
// a classic read has min(primary segments, task slots) partitions (common.scala: cbSegments, taskSlots)
:load /regression/probes/common.scala
import org.apache.spark.scheduler._
val segments = math.min(cbSegments, taskSlots)
val stageTasks = scala.collection.mutable.ArrayBuffer[Int]()
spark.sparkContext.addSparkListener(new SparkListener { override def onStageCompleted(e: SparkListenerStageCompleted): Unit = stageTasks.synchronized { stageTasks += e.stageInfo.numTasks } })
def lastStageTasks(): Int = { Thread.sleep(3000); stageTasks.synchronized { stageTasks.last } }
check("read without parallelism: rows", 10L) { cb("its_payments").count() }
check("read without parallelism: partitions (= min(segments, task slots))", segments) { cb("its_payments").rdd.getNumPartitions }
check("read parallelism=1: partitions", 1) { cb("its_payments", Map("parallelism" -> "1")).rdd.getNumPartitions }
check("read parallelism=1: rows", 10L) { cb("its_payments", Map("parallelism" -> "1")).count() }
val local = spark.createDataFrame(spark.sparkContext.parallelize(cb("its_payments").collect().toSeq, 4), cb("its_payments").schema).localCheckpoint()
def write(p: String): String = { local.write.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy") + ("truncate" -> "true") + ("parallelism" -> p)).mode("overwrite").save(); s"tasks ${lastStageTasks()}, rows ${cb("its_payments_copy").count()}" }
check("write 4 partitions, parallelism=2", "tasks 2, rows 10") { write("2") }
check("write 4 partitions, parallelism=1", "tasks 1, rows 10") { write("1") }
// write.repartition=false: no shuffle — a write task per input partition (more than segments: INSERT waves)
check("write 4 partitions, write.repartition=false", "tasks 4, rows 10") {
  local.write.format("cloudberry").options(opts + ("dbtable" -> "its_payments_copy") + ("truncate" -> "true") + ("write.repartition" -> "false")).mode("overwrite").save()
  s"tasks ${lastStageTasks()}, rows ${cb("its_payments_copy").count()}" }
checkFails("read parallelism=-1", "parallelism must be an integer") { cb("its_payments", Map("parallelism" -> "-1")).count() }
done()
:quit

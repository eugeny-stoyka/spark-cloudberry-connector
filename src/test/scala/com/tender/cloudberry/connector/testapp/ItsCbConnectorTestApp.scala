package com.tender.cloudberry.connector.testapp

import org.apache.spark.sql._
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import org.apache.spark.sql.functions.{udf, col}
import java.time._
import java.util.UUID.randomUUID
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.{Map => ImmutableMap}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object ItsCbConnectorTestApp {

  val gf = new GeometryFactory
  val rnd = new scala.util.Random
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  def getPoint(x: Double, y: Double): String = {
    val wkbWriter = new WKBWriter(2, true)
    try {
      val p = gf.createPoint(new Coordinate(x, y))
      p.setSRID(4326)
      WKBWriter.toHex(wkbWriter.write(p))
    } catch {
      case e: Exception =>
        val p = gf.createPoint(new Coordinate(0.0, 0.0))
        p.setSRID(4326)
        WKBWriter.toHex(wkbWriter.write(p))
    }
  }

  def pointToString(pointHex: String): String = {
    val wkbReader = new WKBReader(gf)
    val point: Geometry = wkbReader.read(WKBReader.hexToBytes(pointHex))
    val coordinates = point.getCoordinates
    s"POINT(${coordinates(0).x} ${coordinates(0).y})"
  }

  def usage(): Unit = {
    println("Usage:")
    println("-h | --help : Print this usage and exit")
    println("-n | --num-rows <number of rows to generate> - default 10000")
    println("-m | --mode <append|overwrite|..> - default append or use spark.cloudberry.mode config param")
    println("-l | --log-level <INFO|WARN|..>")
    println("-p | --num-partitions <number of partitions in the generated data frame> - default 2")
  }

  type OptionMap = Map[Symbol, String]

  def parseArgs(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case x :: tail if x == "-h" || x == "--help" => usage(); sys.exit(0)
      case y :: option :: tail if y == "-n" || y == "--num-rows" => parseArgs(map ++ Map('nRows -> option), tail)
      case y :: option :: tail if y == "-m" || y == "--mode" => parseArgs(map ++ Map('mode -> option), tail)
      case y :: option :: tail if y == "-l" || y == "--log-level" => parseArgs(map ++ Map('logLevel -> option), tail)
      case y :: option :: tail if y == "-p" || y == "--num-partitions" => parseArgs(map ++ Map('nPartitions -> option), tail)
    }
  }

  def main(args: Array[String]): Unit = {
    val options: ImmutableMap[Symbol, String] = parseArgs(ImmutableMap[Symbol, String](), args.toList)
    val getGeom = udf[String, Double, Double](getPoint)
    val pointToStr = udf[String, String](pointToString)
    val spark = SparkSession.builder().appName("cloudberry-connector-test").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val nRowsGenerate: Int = options.getOrElse('nRows, "10000").toInt
    val cbUrl = spark.conf.get("spark.cloudberry.jdbc.url", "jdbc:postgresql://al-desktop:5432/bigdata")
    val cbUser = spark.conf.get("spark.cloudberry.user", "al")
    val cbPassword = spark.conf.get("spark.cloudberry.password", "")
    val cbTable = spark.conf.get("spark.cloudberry.dbtable", "uuid_seq")
    val cbInsertMode = options.getOrElse('mode, spark.conf.get("spark.cloudberry.mode", "append"))
    val cbBufferSize = spark.conf.get("spark.cloudberry.buffer.size", "")
    val cbOptionsMap = MutableMap[String, String]()
    cbOptionsMap.put("url", cbUrl)
    cbOptionsMap.put("user", cbUser)
    cbOptionsMap.put("dbtable", cbTable)
    if (cbPassword.nonEmpty)
      cbOptionsMap.put("password", cbPassword)
    if (cbBufferSize.nonEmpty)
      cbOptionsMap.put("buffer.size", cbBufferSize)

    val logLevel: String = options.getOrElse('logLevel, "")
    if (logLevel.nonEmpty) {
      sc.setLogLevel(logLevel)
    }
    val numSlices: Int = options.getOrElse('nPartitions, "2").toInt
    logger.info(s"Generating ${nRowsGenerate} records spread into ${numSlices} partitions..")
    val df = sc.parallelize(Array.fill[String](nRowsGenerate) {
      randomUUID().toString
    }.zipWithIndex.map(
      { case (uid, id) => {
        (uid,
          id,
          java.sql.Timestamp.from(OffsetDateTime.now().toInstant),
          id % 2 == 1,
          getPoint(rnd.nextInt( (60 - 10) + 1 ), rnd.nextInt( (60 - 10) + 1 ))
        )
      }
      }
    ), numSlices)
      .toDF("id", "seq_no", "created_d", "mv", "gps_point")
    df.withColumn("gps_point_fmt", pointToStr(col("gps_point"))).show(20, false)
    df.write.format("cloudberry")
      //.option("url", cbUrl).option("user", "al").option("dbtable", "uuid_seq")
      .options(cbOptionsMap.toMap)
      .mode(cbInsertMode) //SaveMode.Append)
      .save()
    println(s"Done")
  }
}

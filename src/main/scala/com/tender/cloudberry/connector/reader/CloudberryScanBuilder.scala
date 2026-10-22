package com.tender.cloudberry.connector.reader

import com.tender.cloudberry.connector.{CBClient, CloudberryRowSet}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.tender.cloudberry.connector.{CBOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class CloudberryScanBuilder(optionsFactory: CBOptionsFactory, rowSet: CloudberryRowSet, var schema: StructType)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with Logging
{
  private var whereClause = ""
  private var pushedDownFilters: Array[Filter] = new Array[Filter](0)
  logDebug(s"""options=\n${optionsFactory.dumpParams()}""")
  private var cloudberryScan: CloudberryScan = null

  override def build(): Scan = this.synchronized {
    if (cloudberryScan == null)
      cloudberryScan = new CloudberryScan(optionsFactory, rowSet, schema, whereClause)
    cloudberryScan
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val tuple3 = SparkSchemaUtil(optionsFactory.dbTimezone).pushFilters(filters)
    whereClause = tuple3._1
    pushedDownFilters = tuple3._3
    tuple3._2
  }

  override def pushedFilters(): Array[Filter] = pushedDownFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    schema = requiredSchema
  }
}

package com.tender.cloudberry.connector

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util
import org.apache.spark.sql.types.StructType

/**
 * tender: the data source "cloudberry" — spark.read.format("cloudberry"). Registered in
 * META-INF/services/org.apache.spark.sql.sources.DataSourceRegister.
 * The classes are its own (com.tender.cloudberry.connector): no clash with another data source on the classpath.
 */
class CloudberryDataSource extends TableProvider
  with DataSourceRegister {

  override def shortName(): String = "cloudberry"

  /**
   * tender: a schema given by the user (spark.read.schema(...)) is taken — e.g. a json / jsonb column read as a
   * map / struct / array; without it the schema comes from Cloudberry (inferSchema). A write gets the schema of
   * its DataFrame.
   */
  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val rowSet = new CloudberryRowSet(null, Array.empty[Transform], options.asCaseSensitiveMap())
    val ret: StructType = rowSet.schema()
    rowSet.clear()
    ret
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new CloudberryRowSet(schema, partitioning, properties)
  }
}

package org.apache.spark.sql.tender.cloudberry.connector

object CbTableTypes extends Enumeration {
  /**
   * Denotes a type of Cloudberry table, one of:
   * <p><b>None</b> - means "Unspecified"
   * <p><b>Target</b> - a table that can hold final source or destination data
   * <p><b>ExternalReadable</b> - Cloudberry readable external table for GPFDIST protocol
   * <p><b>ExternalWritable</b> - Cloudberry writable external table for GPFDIST protocol
   */
  type CbTableType = Value
  val None, Target, ExternalReadable, ExternalWritable = Value
}

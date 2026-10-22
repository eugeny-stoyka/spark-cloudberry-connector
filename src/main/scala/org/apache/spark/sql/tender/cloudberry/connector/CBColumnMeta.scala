package org.apache.spark.sql.tender.cloudberry.connector

case class CBColumnMeta(sqlTypeId: Int, dbTypeName: String, nullable: Int, colSize: Integer, decimalDigits: Integer) {}

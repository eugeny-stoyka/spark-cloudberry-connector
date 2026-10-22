package org.apache.spark.sql.tender.cloudberry.connector

case class CBTarget(tableOrQuery: String) {

  val (cleanDbTableName: String, tableSchema: String, isQuery: Boolean, isQuated: Boolean) = guessTargetFlags()

  private def guessTargetFlags(): (String, String, Boolean, Boolean) = {
    var cleanDbTableName: String = ""
    var tableSchema: String = ""
    var isQuery: Boolean = false
    var isQuated: Boolean = false
    val queryPattern = """\A\s*(?i)(select\s+\S.*)""".r
    val tablePattern = """\A(?i)(\w+[.])?(\w+)\z""".r
    val quotedTablePattern = """\A(?i)(["][^"]+["][.])?(["][^"]+["])\z""".r
    tableOrQuery match {
      case queryPattern(_) => isQuery = true
      case tablePattern(prefix, tblName) => cleanDbTableName = tblName
        if (prefix != null && prefix.nonEmpty) {
          tableSchema = prefix.dropRight(1)
        }
      case quotedTablePattern(prefix, tblName) => cleanDbTableName = tblName
        if (prefix != null && prefix.nonEmpty) {
          tableSchema = prefix.dropRight(1)
        }
        isQuated = true
      case _ =>
    }
    (cleanDbTableName, tableSchema, isQuery, isQuated)
  }

  /**
   * If this instance provides DB schema, then use it as is, otherwise substitutes defaultSchema prefix
   * @param defaultSchema default DB schema (object search path) of the current user session
   * @return DBschema.tableName
   */
  def getCanonicalName(defaultSchema: String): String = {
    if (tableSchema.nonEmpty) {
      return tableOrQuery
    } else if (cleanDbTableName.nonEmpty) {
      if (defaultSchema != null && defaultSchema.nonEmpty)
        return s"${defaultSchema}.${cleanDbTableName}"
      else return cleanDbTableName
    }
    ""
  }

  /**
   * If this instance provides DB schema, then use it as is, otherwise substitutes defaultSchema prefix
   *
   * @param defaultSchema default DB schema (object search path) of the current user session
   * @return DBschema
   */
  def getTableSchema(defaultSchema: String): String = {
    if (tableSchema.nonEmpty) {
      return tableSchema
    } else if (cleanDbTableName.nonEmpty) {
      return if (defaultSchema != null) defaultSchema else ""
    }
    ""
  }
}

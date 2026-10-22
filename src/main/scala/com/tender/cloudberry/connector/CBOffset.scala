package com.tender.cloudberry.connector

import org.apache.spark.sql.connector.read.streaming.Offset
//org.apache.spark.sql.sources.v2.reader.streaming.Offset

import java.sql.Connection
import java.time.LocalDateTime

case class CBOffset(private var value: Option[String] = None) extends Offset {

  override def json(): String = this.synchronized {
    value.getOrElse("[null]")
  }

  def get(): Option[String] = this.synchronized {
    value
  }

  def set(newValue: Option[String]): Unit = this.synchronized {
    value = newValue
  }

  def store(conn: Connection, sql: String): Unit = this.synchronized {
    if (sql == null || sql.isEmpty)
      return
    CBClient.using(conn.prepareStatement(sql)) {
      statement => {
        val offset: String = json()
        statement.setString(1, if (offset == "[null]") null else offset)
        statement.executeUpdate()
      }
    }
  }

  def extract(conn: Connection, sql: String): Unit = this.synchronized {
    if (sql == null || sql.isEmpty) {
      value = Option(s"""{"timestamp":"${LocalDateTime.now()}"}""")
      return
    }
    CBClient.using(conn.createStatement()) {
      statement => {
        CBClient.using(statement.executeQuery(sql)) {
          rs => {
            while (rs.next()) {
              value = Option(rs.getString(1))
            }
          }
        }
      }
    }
  }
}

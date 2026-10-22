package com.tender.cloudberry.connector.reader

import com.tender.cloudberry.connector.CBClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.tender.cloudberry.connector.CBOptionsFactory

import java.sql.Connection
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import scala.collection.mutable
import scala.util.Try

/**
 * tender: the snapshots of the executions of a split read (read.split.snapshot). An execution (a run of the
 * stage of the scan) exports one on the driver — pg_export_snapshot() in a REPEATABLE READ transaction kept
 * open — and every part imports it (SET TRANSACTION SNAPSHOT): all the parts, retries included, see the same
 * data. Released when all the parts of the execution are done, at the stop of the scan, or after idleMs without
 * a part running (a failed job, a limit that read a few parts only). A server that cannot export one (Greenplum
 * before 6.21) — a warning, every part reads its own snapshot.
 *
 * @param parts the parts of the read
 */
private[reader] class SplitSnapshots(optionsFactory: CBOptionsFactory, parts: Int, idleMs: Long) extends Logging {

  private class Holder(val conn: Connection, val id: String) {
    val done: mutable.Set[Int] = mutable.Set[Int]()
    var running = 0
    var lastUsedMs: Long = System.currentTimeMillis()
  }

  private val holders = mutable.Map[String, Holder]()
  /** executions whose snapshot was released before all their parts were done */
  private val released = mutable.Set[String]()
  private var unsupported = false
  private var idleCheck: ScheduledFuture[_] = null

  /** The snapshot of the execution for a part starting, None — not available */
  def acquire(execution: String): Option[String] = synchronized {
    if (unsupported)
      return None
    holders.get(execution).orElse(export(execution)).map { h =>
      h.running += 1
      h.lastUsedMs = System.currentTimeMillis()
      h.id
    }
  }

  /** A part that acquired the snapshot has ended; done — successfully */
  def release(execution: String, part: Int, done: Boolean): Unit = synchronized {
    holders.get(execution).foreach { h =>
      h.running -= 1
      h.lastUsedMs = System.currentTimeMillis()
      if (done)
        h.done += part
      if (h.done.size >= parts)
        close(execution, h)
    }
  }

  def close(): Unit = synchronized {
    holders.toSeq.foreach { case (execution, h) => close(execution, h) }
  }

  private def export(execution: String): Option[Holder] = {
    var conn: Connection = null
    try {
      conn = CBClient.getConn(optionsFactory)
      conn.setAutoCommit(false)
      conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      val st = conn.createStatement()
      val id = try {
        val rs = st.executeQuery("select pg_export_snapshot()")
        rs.next()
        rs.getString(1)
      } finally st.close()
      if (released.contains(execution))
        logWarning(s"read.split.snapshot: the snapshot of the execution $execution was released after $idleMs ms " +
          "without a part running — its remaining parts read a new one")
      val holder = new Holder(conn, id)
      holders.put(execution, holder)
      if (idleCheck == null)
        idleCheck = SplitSnapshots.scheduler.scheduleWithFixedDelay(() => closeIdle(), 10, 10, TimeUnit.SECONDS)
      logDebug(s"Split read execution $execution: snapshot $id")
      Some(holder)
    } catch {
      case e: Exception =>
        unsupported = true
        logWarning(s"read.split.snapshot: no exported snapshot in this Cloudberry ($e): every part of the read " +
          "reads its own snapshot")
        if (conn != null)
          Try(conn.close())
        None
    }
  }

  private def closeIdle(): Unit = synchronized {
    val now = System.currentTimeMillis()
    holders.toSeq.foreach { case (execution, h) =>
      if (h.running == 0 && now - h.lastUsedMs > idleMs) {
        logInfo(s"Split read execution $execution: snapshot released, ${h.done.size} of $parts parts done, " +
          s"none running for $idleMs ms")
        released += execution
        close(execution, h)
      }
    }
  }

  private def close(execution: String, h: Holder): Unit = {
    holders.remove(execution)
    Try(h.conn.rollback())
    Try(h.conn.close())
    if (holders.isEmpty && idleCheck != null) {
      idleCheck.cancel(false)
      idleCheck = null
    }
  }
}

private[reader] object SplitSnapshots {
  private val scheduler = Executors.newSingleThreadScheduledExecutor((r: Runnable) => {
    val t = new Thread(r, "cb-split-snapshots")
    t.setDaemon(true)
    t
  })
}

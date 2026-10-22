package com.tender.cloudberry.connector.transfer

import org.apache.spark.internal.Logging

import java.nio.ByteBuffer
import java.sql.{Connection, Statement}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, ExecutionException, Executors}
import scala.util.Try

/**
 * tender: a relay of write.direct — the gpfdist endpoint the segments of the source POST their rows to (a writable
 * external table) and the segments of the target GET them from (a readable one). The rows go as they came, whole
 * lines of the TEXT format: no Spark task, no parsing. Bounded (buffer.max): a fast source waits for the target.
 */
class RelayChannel(bufferSize: Int, bufferMax: Long) extends GpfdistChannel {
  private val failure = new AtomicReference[String](null)
  private val finished = new AtomicBoolean(false)
  private val buffer = new BufferExchange(bufferSize, bufferMax, () => failure.get != null)
  /** the lines passed from the source */
  val rows = new AtomicLong(0)

  override def received(data: Array[Byte]): Unit = {
    var lines = 0L
    var i = 0
    while (i < data.length) {
      if (data(i) == '\n')
        lines += 1
      i += 1
    }
    if (!buffer.awaitSpaceUnless(() => false))
      throw new IllegalStateException(s"relay aborted: ${failure.get}")
    buffer.put(data)
    rows.addAndGet(lines)
  }

  override def segmentDone(): Unit = buffer.flush()

  /** the next chunk for a segment of the target; null — the end: the source is done and the buffer is empty */
  override def nextChunk(): ByteBuffer = {
    while (true) {
      val chunk = buffer.get(100)
      if (chunk != null)
        return chunk
      if (failure.get != null)
        return null
      if (finished.get()) {
        buffer.flush()
        return buffer.get(-1)
      }
    }
    null
  }

  override def recycle(buff: ByteBuffer): Unit = buffer.addBuffer(buff)

  override def isAborted: Boolean = failure.get != null

  override def abortTransfer(reason: String): Unit = failure.compareAndSet(null, reason)

  /** the INSERT of the source is done: the rest of the buffer, then the end of the streams of the target */
  def finish(): Unit = finished.set(true)
}

/**
 * tender: write.direct between two sessions — other clusters or other users, each side under its own credentials:
 * relays on the driver, the INSERT of the source into a writable external table with their locations and the INSERT
 * of the target from a readable one with the same locations, both at once. The end: when the INSERT of the source is
 * done, the relays end the streams of the target. A failure of one side aborts the relays and cancels the other;
 * the row counts of both INSERTs and of the relays are compared. The target connection is left to the caller to
 * commit (its transaction), the source one is used in autocommit.
 */
object GpfdistRelay extends Logging {

  /** the SQL of a side for the locations of the relays: its external table, the INSERT, the drop of the table */
  case class Side(create: Seq[String] => String, insert: Seq[String] => String, drop: String)

  /** @return the rows inserted into the target */
  def run(relays: Int, portRange: (Int, Int), bufferSize: Int, bufferMax: Long,
          source: Connection, sourceSide: Side, target: Connection, targetSide: Side): Long = {
    val channels = (0 until relays).map(_ => new RelayChannel(bufferSize, bufferMax))
    val servers = channels.map(c => new GpfdistServer(portRange, c, new WebServerMetrics))
    val host = NetUtils().getLocalHostNameAndIp._2
    val urls = servers.map(s => s"gpfdist://$host:${s.httpPort}/relay")
    val statements = new ConcurrentLinkedQueue[Statement]()
    val pool = Executors.newFixedThreadPool(2, TransferCoordinator.daemonThreads("cloudberry-relay"))
    def abort(reason: String): Unit = {
      channels.foreach(_.abortTransfer(reason))
      statements.forEach(st => Try(st.cancel()))
    }
    def update(conn: Connection, sql: String): Long = {
      val st = conn.createStatement()
      statements.add(st)
      try st.executeLargeUpdate(sql) finally {
        statements.remove(st)
        st.close()
      }
    }
    def side(name: String)(f: => Long): Callable[Long] = new Callable[Long] {
      override def call(): Long = try f catch {
        case e: Throwable =>
          abort(s"the INSERT of the $name failed: $e")
          throw e
      }
    }
    var sourceCreated = false
    try {
      update(target, targetSide.create(urls))
      update(source, sourceSide.create(urls))
      sourceCreated = true
      val inserted = pool.submit(side("target")(update(target, targetSide.insert(urls))))
      val sent = pool.submit(side("source") {
        val n = update(source, sourceSide.insert(urls))
        channels.foreach(_.finish())
        n
      })
      val (sentRows, insertedRows) = try {
        (sent.get(), inserted.get())
      } catch {
        case e: ExecutionException => throw e.getCause
      }
      val relayed = channels.map(_.rows.get).sum
      if (sentRows != insertedRows || relayed != sentRows)
        throw new IllegalStateException(s"row count mismatch: the source sent $sentRows rows, the relays passed " +
          s"$relayed, the target inserted $insertedRows")
      Try(update(target, targetSide.drop))
      logInfo(s"write.direct relay: $insertedRows rows through ${channels.size} relays at $host")
      insertedRows
    } catch {
      case e: Throwable =>
        abort(s"write.direct failed: $e")
        throw e
    } finally {
      pool.shutdownNow()
      servers.foreach(_.stop())
      if (sourceCreated)
        Try(update(source, sourceSide.drop))
    }
  }
}

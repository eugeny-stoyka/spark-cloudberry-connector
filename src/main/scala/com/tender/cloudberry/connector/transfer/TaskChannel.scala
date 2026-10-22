package com.tender.cloudberry.connector.transfer

import com.tender.cloudberry.connector.transfer.TaskState.{Aborted, Done, Pending, Transferred}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.tender.cloudberry.connector.CBOptionsFactory

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

/**
 * tender: the transfer endpoint of one Spark task (replaces RMISlave): its own gpfdist server, a bounded
 * buffer between Cloudberry and the task, the registration with the driver [[TransferCoordinator]] and
 * a poller of the batch outcome.
 *
 * Read (Cloudberry -> Spark): segments POST rows, the task reads them; the end of data — the driver says
 * the INSERT into the writable external table is done and the buffer is drained.
 * Write (Spark -> Cloudberry): the task writes rows, segments GET them; the end of data — finish() and the
 * buffer is drained; the task then waits for the INSERT of its batch (awaitDone).
 *
 * @param run   the transfer run (a stage execution, an epoch) the task belongs to
 * @param runSize number of partitions (tasks) of the run
 */
class TaskChannel(optionsFactory: CBOptionsFactory,
                  coordinatorAddress: String,
                  sessionId: String,
                  run: String,
                  epoch: Long,
                  part: Int,
                  attempt: Int,
                  runSize: Int,
                  readTransfer: Boolean)
  extends GpfdistChannel with Logging
{
  private val key = TaskKey(run, part, attempt)
  private val aborted = new AtomicBoolean(false)
  private val abortReason = new AtomicReference[String](null)
  private val finished = new AtomicBoolean(false)
  private val discard = new AtomicBoolean(false)
  private val closed = new AtomicBoolean(false)
  private val outcome = new AtomicReference[TaskState](Pending)
  /** rows sent (write) / received (read), reported to the driver for the row count check */
  private val rowCount = new AtomicLong(0)
  private val reported = new AtomicBoolean(false)
  val putWaitMs = new AtomicLong(0)
  val getWaitMs = new AtomicLong(0)
  val metrics = new WebServerMetrics()

  private val buffer = new BufferExchange(optionsFactory.bufferSize, optionsFactory.bufferMax,
    () => aborted.get() || isOutcomeAborted)
  // tender: server.compress — Cloudberry segments send the rows of a read zstd-compressed
  private val server = new GpfdistServer(optionsFactory.serverPortRange, this, metrics,
    compress = readTransfer && optionsFactory.serverCompress)
  val gpfdistUrl: String = s"gpfdist://${NetUtils().getLocalHostNameAndIp._2}:${server.httpPort}/output.pipe"
  private val client = new CoordinatorClient(coordinatorAddress, sessionId, key, epoch, runSize)

  try {
    client.register(gpfdistUrl)
  } catch {
    case e: Exception =>
      server.stop()
      throw e
  }
  logDebug(s"Task $key of session $sessionId: gpfdist at $gpfdistUrl")

  /** Long-polls the outcome of the batch; a coordinator unreachable for network.timeout aborts the task */
  private val poller = new Thread(new Runnable {
    override def run(): Unit = {
      var failingSince = 0L
      while (!closed.get() && !outcome.get().terminal) {
        try {
          outcome.set(client.status(1000, outcome.get()))
          failingSince = 0L
        } catch {
          case e: Exception =>
            if (failingSince == 0L) failingSince = System.currentTimeMillis()
            if (System.currentTimeMillis() - failingSince > optionsFactory.networkTimeout)
              outcome.set(Aborted(s"transfer coordinator $coordinatorAddress unreachable: ${e.getMessage}"))
            else Thread.sleep(200)
        }
      }
      if (discard.get()) // the reader stopped early: Cloudberry has finished, drop the endpoint now
        shutdown()
    }
  }, s"cb-transfer-status-$sessionId-$part")
  poller.setDaemon(true)
  poller.start()

  private def isOutcomeAborted: Boolean = outcome.get().isInstanceOf[Aborted]

  private def failure: String = Option(abortReason.get()).getOrElse(outcome.get() match {
    case Aborted(reason) => reason
    case other => other.render
  })

  private def checkAborted(): Unit =
    if (aborted.get() || isOutcomeAborted)
      throw new IllegalStateException(s"Cloudberry transfer aborted: $failure")

  /** Once: the rows of the task for the driver check (-1 — unknown) */
  private def reportRows(rows: Long): Unit =
    if (reported.compareAndSet(false, true)) {
      try client.finish(rows) catch {
        case e: Exception => logWarning(s"Task $key: row report to $coordinatorAddress failed: $e")
      }
    }

  /** Waits for the final state after the row report: Done, or throws */
  private def awaitFinal(maxMs: Long): Unit = {
    val start = System.currentTimeMillis()
    while (!outcome.get().terminal && !aborted.get()) {
      if (maxMs > 0 && System.currentTimeMillis() - start > maxMs)
        throw new IllegalStateException(s"Cloudberry transfer of task $key not done in $maxMs ms")
      Thread.sleep(10)
    }
    checkAborted()
  }

  // ---------- GpfdistChannel (gpfdist server threads) ----------

  override def nextChunk(): ByteBuffer = {
    while (true) {
      val chunk = buffer.get(100)
      if (chunk != null)
        return chunk
      if (aborted.get() || isOutcomeAborted)
        return null // stop sending: the driver rolls the transaction back
      if (finished.get()) {
        buffer.flush()
        return buffer.get(-1) // null — all sent
      }
    }
    null
  }

  override def recycle(buff: ByteBuffer): Unit = buffer.addBuffer(buff)

  override def received(data: Array[Byte]): Unit = {
    if (discard.get())
      return
    var lines = 0L
    var i = 0
    while (i < data.length) {
      if (data(i) == '\n')
        lines += 1
      i += 1
    }
    if (buffer.awaitSpaceUnless(() => discard.get())) {
      buffer.put(data)
      rowCount.addAndGet(lines)
    } else if (!discard.get()) {
      throw new IllegalStateException(s"Cloudberry transfer aborted: $failure")
    } // else the reader has gone: drop the rows
  }

  override def segmentDone(): Unit = buffer.flush()

  override def isAborted: Boolean = aborted.get() || isOutcomeAborted

  override def abortTransfer(reason: String): Unit = abort(reason)

  // ---------- the task ----------

  /**
   * Write: the next rows — the first `length` bytes of the block, `rows` lines, copied: the task reuses the block
   * (backpressure: waits while buffer.max bytes are queued). tender: a block of lines (TextRowWriter), not a row:
   * before, every row took the locks of the buffer, two clock reads and two atomic counters
   */
  def writeRows(block: Array[Byte], length: Int, rows: Long): Unit = {
    val start = System.currentTimeMillis()
    checkAborted()
    buffer.awaitSpace()
    buffer.put(block, length)
    rowCount.addAndGet(rows)
    putWaitMs.addAndGet(System.currentTimeMillis() - start)
  }

  /** Write: no more rows; reports their count for the check of the batch INSERT */
  def finish(): Unit = {
    buffer.flush()
    finished.set(true)
    reportRows(rowCount.get())
  }

  /** Write: waits for the INSERT of the task batch; maxMs <= 0 — while the coordinator is reachable */
  def awaitDone(maxMs: Long): Unit = {
    val start = System.currentTimeMillis()
    while (!outcome.get().terminal && !aborted.get()) {
      if (maxMs > 0 && System.currentTimeMillis() - start > maxMs)
        throw new IllegalStateException(s"Cloudberry transfer of task $key not done in $maxMs ms")
      Thread.sleep(20)
    }
    getWaitMs.addAndGet(System.currentTimeMillis() - start)
    checkAborted()
  }

  /**
   * Read: the next chunk of complete rows, null — end of data: Cloudberry has pushed everything
   * (Transferred), the buffer is drained, and the driver has checked the row count of the run (Done)
   */
  def read(): ByteBuffer = {
    val start = System.currentTimeMillis()
    try {
      while (true) {
        val chunk = buffer.get(100)
        if (chunk != null)
          return chunk
        checkAborted()
        val state = outcome.get()
        if (state == Transferred || state == Done) {
          buffer.flush()
          val rest = buffer.get(-1)
          if (rest != null)
            return rest
          reportRows(rowCount.get())
          awaitFinal(0)
          return null
        }
      }
      null
    } finally {
      getWaitMs.addAndGet(System.currentTimeMillis() - start)
    }
  }

  /**
   * Read: the task stops before the end of data (a limit, a cancelled job) — the rest is dropped and the
   * gpfdist server stays until Cloudberry has finished, so the INSERT of the other tasks is not broken.
   */
  def discardRest(): Unit = {
    reportRows(-1) // nothing to check for this run
    if (outcome.get().terminal) {
      close()
    } else {
      discard.set(true)
      // drain from the consumer side (safe next to a producer, unlike clear) — frees the memory and
      // wakes a POST waiting for space; later POSTs are dropped in received
      var chunk = buffer.get(-1)
      while (chunk != null) {
        chunk = buffer.get(-1)
      }
      logDebug(s"Task $key: reader closed early, the rest of its data is discarded")
    }
  }

  def abort(reason: String): Unit = {
    if (aborted.compareAndSet(false, true)) {
      abortReason.set(reason)
      logWarning(s"Cloudberry transfer task $key of session $sessionId aborted: $reason")
      try client.abort(reason) catch {
        case e: Exception => logWarning(s"Abort notification to $coordinatorAddress failed: $e")
      }
    }
    close()
  }

  def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      poller.interrupt()
      shutdown()
    }
  }

  private def shutdown(): Unit = {
    server.stop() // the buffer goes with the channel (no clear: a stopping POST may still put into it)
  }
}

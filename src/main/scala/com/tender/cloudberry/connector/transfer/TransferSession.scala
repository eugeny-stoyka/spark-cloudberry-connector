package com.tender.cloudberry.connector.transfer

import com.tender.cloudberry.connector.transfer.TaskState.{Aborted, Done, Transferred}
import org.apache.spark.internal.Logging

object TransferSession {
  /** sessions without any activity that long are closed by the coordinator (abandoned scans) */
  val maxIdleMs: Long = 60L * 60 * 1000
}

/**
 * tender: transfer state of one read scan / write on the driver.
 *
 * Tasks of a run (a stage execution of a scan, a micro-batch epoch, a write) register their gpfdist
 * servers; registered tasks form a batch — one transfer SQL (runBatch) with their locations — when all
 * tasks of the run are there or when none came for settleMs (not all of them run at once: slots are busy,
 * or Spark runs a few partitions only — limit / take / show). At most maxRunning batches run at once,
 * each of its own run: 1 — one after another; a split read (read.split) — every part (a run of one task)
 * on its own.
 *
 * Read (readTransfer): one batch per run — Cloudberry pushes the whole result into the locations of that
 * batch; tasks of the run registering later get no data (done at once). A batch whose readers have all
 * stopped early (a limit) is cancelled, so is the SQL of an aborted run. A failure aborts the run only
 * (the scan may run again, a stream has next epochs, a part of a split read has its next attempt).
 * Write: at most maxBatch tasks per batch (Cloudberry allows no more locations than primary segments),
 * waves while tasks remain; all batches are one transaction of the write (the caller commits / rolls it
 * back); a failure aborts the whole session.
 *
 * Row count check: runBatch returns the rows Cloudberry has transferred (None — not comparable, e.g. a
 * custom sqltransfer); the tasks report the rows they have sent (write) / received (read, after draining
 * their buffers — the Transferred state); a mismatch fails the batch instead of losing rows silently.
 * The tasks see Done only after the check.
 */
class TransferSession(val id: String,
                      readTransfer: Boolean,
                      maxBatch: Int,
                      settleMs: Long,
                      registerTimeoutMs: Long,
                      runBatch: TransferBatch => Option[Long],
                      maxRunning: Int = 1,
                      isolatedRuns: Boolean = false) extends Logging {
  /** how long the tasks of a batch may take to report their rows (a reader drains its buffer first) */
  private val reportTimeoutMs = math.max(registerTimeoutMs, 600000L)
  private val lock = new Object
  private val registrations = scala.collection.mutable.LinkedHashMap[(String, Int), Registration]()
  private val runSizes = scala.collection.mutable.Map[String, Int]()
  /** run -> when all its tasks got a final state; such runs are forgotten a minute later */
  private val runFinishedAt = scala.collection.mutable.Map[String, Long]()
  /** run -> its batch running now (at most maxRunning) */
  private val running = scala.collection.mutable.LinkedHashMap[String, (TransferBatch, Seq[Registration])]()
  /** runs whose running SQL is cancelled on purpose: all readers stopped early, or the run is aborted */
  private val cancelled = scala.collection.mutable.Set[String]()
  private var aborted: Option[String] = None
  private var closed = false
  private var lastRegistrationMs = 0L
  @volatile private var lastActivityMs = System.currentTimeMillis()

  def idleMs: Long = if (lock.synchronized(running.nonEmpty)) 0 else System.currentTimeMillis() - lastActivityMs

  def register(key: TaskKey, epoch: Long, runSize: Int, url: String): Unit = lock.synchronized {
    lastActivityMs = System.currentTimeMillis()
    aborted.foreach(reason => throw new IllegalStateException(s"transfer aborted: $reason"))
    val slot = (key.run, key.part)
    registrations.get(slot) match {
      case Some(previous) if previous.batched || previous.state.terminal =>
        // a retry after the data of this partition went into a batch: the transfer cannot be repeated
        // partially — abort (Spark fails the job; for a write the transaction is rolled back)
        val reason = s"partition ${key.part} of run ${key.run} restarted (attempt ${key.attempt}) " +
          s"after its transfer had begun"
        failLocked(key.run, reason)
        throw new IllegalStateException(reason)
      case _ =>
    }
    val registration = Registration(key, epoch, url)
    if (readTransfer && runBatched(key.run)) {
      // the data of this run has gone (or goes) to the tasks of its batch: nothing for this one
      registration.batched = true
      registration.state = Done
      registration.rows = Some(0L)
    }
    registrations.put(slot, registration)
    runSizes.getOrElseUpdate(key.run, runSize)
    lastRegistrationMs = System.currentTimeMillis()
    logDebug(s"Session $id: registered $key at $url${if (registration.state == Done) " (late, no data)" else ""}")
    lock.notifyAll()
    scheduleLocked()
  }

  /**
   * A write task without rows (it registers lazily, at its first row): done at once, counted for its run,
   * no location in any batch — Cloudberry does not open an empty stream to it
   */
  def registerEmpty(key: TaskKey, epoch: Long, runSize: Int): Unit = lock.synchronized {
    lastActivityMs = System.currentTimeMillis()
    aborted.foreach(reason => throw new IllegalStateException(s"transfer aborted: $reason"))
    val slot = (key.run, key.part)
    registrations.get(slot) match {
      case Some(previous) if previous.batched =>
        val reason = s"partition ${key.part} of run ${key.run} restarted (attempt ${key.attempt}) " +
          s"after its transfer had begun"
        failLocked(key.run, reason)
        throw new IllegalStateException(reason)
      case _ =>
    }
    registrations.put(slot, Registration(key, epoch, url = "", batched = true, state = Done, rows = Some(0L)))
    runSizes.getOrElseUpdate(key.run, runSize)
    logDebug(s"Session $id: empty task $key")
    lock.notifyAll()
    scheduleLocked() // the waiting tasks may be all the run has now
  }

  /** Long poll: returns as soon as the state is not `seen` any more (or is final), or after waitMs */
  def awaitStatus(key: TaskKey, waitMs: Long, seen: String = ""): TaskState = lock.synchronized {
    lastActivityMs = System.currentTimeMillis()
    val deadline = System.currentTimeMillis() + waitMs
    var state = stateOf(key)
    while (!state.terminal && state.render == seen && System.currentTimeMillis() < deadline) {
      lock.wait(math.max(1L, deadline - System.currentTimeMillis()))
      state = stateOf(key)
    }
    state
  }

  def taskAborted(key: TaskKey, reason: String): Unit = lock.synchronized {
    lastActivityMs = System.currentTimeMillis()
    registrations.get((key.run, key.part)) match {
      case Some(r) if r.key == key && !r.batched =>
        // not yet in a batch: just forget this attempt (a retry may come)
        registrations.remove((key.run, key.part))
        lock.notifyAll()
      case Some(r) if r.key == key && r.state == Done =>
      case _ =>
        failLocked(key.run, s"task ${key.part} (attempt ${key.attempt}) of run ${key.run} failed: $reason")
    }
  }

  def taskFinished(key: TaskKey, rows: Long): Unit = lock.synchronized {
    lastActivityMs = System.currentTimeMillis()
    registrations.get((key.run, key.part)) match {
      case Some(r) if r.key == key =>
        r.rows = Some(rows)
        lock.notifyAll()
        // all readers of the running batch stopped early (a limit): stop Cloudberry pushing the rest
        running.get(key.run).foreach { case (batch, regs) =>
          if (readTransfer && !cancelled.contains(batch.run) && regs.contains(r) && regs.forall(_.rows.contains(-1L))) {
            cancelled += batch.run
            logInfo(s"Transfer session $id: all readers of run ${batch.run} stopped early, its SQL is cancelled")
            cancelLater(batch)
          }
        }
      case _ =>
    }
  }

  /** Driver: abort everything not done yet (task failure reported by Spark, write abort, scan stop) */
  def abort(reason: String): Unit = lock.synchronized(abortLocked(reason))

  /** Driver: wait until all `expected` partitions of the run are done; throws if the transfer aborted */
  def awaitRunDone(run: String, expected: Int, maxMs: Long): Unit = lock.synchronized {
    val deadline = if (maxMs > 0) System.currentTimeMillis() + maxMs else Long.MaxValue
    def doneCount = registrations.valuesIterator.count(r => r.key.run == run && r.state == Done)
    while (aborted.isEmpty && doneCount < expected) {
      if (System.currentTimeMillis() >= deadline)
        throw new IllegalStateException(s"transfer session $id: only $doneCount of $expected tasks done in $maxMs ms")
      lock.wait(100)
    }
    aborted.foreach(reason => throw new IllegalStateException(s"transfer aborted: $reason"))
  }

  def close(reason: String = "transfer session closed"): Unit = {
    lock.synchronized {
      closed = true
      registrations.valuesIterator.filter(!_.state.terminal).foreach(_.state = Aborted(reason))
      lock.notifyAll()
    }
    TransferCoordinator.unregister(id)
  }

  /** Time based decisions; the coordinator ticker calls it every 100 ms */
  def tick(): Unit = lock.synchronized {
    val now = System.currentTimeMillis()
    // forget runs finished a minute ago: their tasks have seen the outcome (long streams stay small)
    runFinishedAt.filter { case (_, at) => now - at > 60000 }.keys.toSeq.foreach { run =>
      registrations.keys.filter(_._1 == run).toSeq.foreach(registrations.remove)
      runSizes.remove(run)
      runFinishedAt.remove(run)
    }
    runSizes.keys.filter(run => !runFinishedAt.contains(run)).foreach { run =>
      val regs = registrations.valuesIterator.filter(_.key.run == run).toSeq
      if (regs.nonEmpty && regs.forall(_.state.terminal) && (regs.size >= runSizes(run) || readTransfer))
        runFinishedAt.put(run, now)
    }
    if (aborted.isEmpty && !closed)
      scheduleLocked()
  }

  private def runBatched(run: String): Boolean = registrations.valuesIterator.exists(r => r.key.run == run && r.batched)

  private def stateOf(key: TaskKey): TaskState = registrations.get((key.run, key.part)) match {
    case Some(r) if r.key == key => r.state
    case Some(r) => Aborted(s"superseded by attempt ${r.key.attempt}")
    case None => if (closed) Aborted("transfer session closed") else aborted.map(Aborted).getOrElse(Aborted("not registered"))
  }

  /** Starts ready batches while fewer than maxRunning are running */
  private def scheduleLocked(): Unit = {
    if (aborted.nonEmpty || closed)
      return
    var next = if (running.size < maxRunning) nextBatchLocked() else None
    while (next.nonEmpty) {
      val regs = next.get
      regs.foreach(_.batched = true)
      val batch = TransferBatch(regs.head.key.run, regs.head.epoch, regs.map(_.url), regs.map(_.key.part))
      running.put(batch.run, (batch, regs))
      TransferCoordinator.sqlPool.submit(new Runnable {
        override def run(): Unit = execute(batch, regs)
      })
      next = if (running.size < maxRunning) nextBatchLocked() else None
    }
  }

  /** The tasks of the first run (in registration order) that is ready for a batch and not running */
  private def nextBatchLocked(): Option[Seq[Registration]] = {
    val waiting = registrations.values.filter(r => !r.batched && !r.state.terminal && !running.contains(r.key.run)).toSeq
    waiting.map(_.key.run).distinct.iterator.map { run =>
      val regs = waiting.filter(_.key.run == run)
      val remaining = runSizes(run) - registrations.valuesIterator.count(r => r.key.run == run && r.batched)
      val full = if (readTransfer) remaining else math.min(maxBatch, remaining)
      if (regs.size >= full || System.currentTimeMillis() - lastRegistrationMs >= settleMs)
        Some(regs.sortBy(_.key.part).take(if (readTransfer) Int.MaxValue else maxBatch))
      else
        None
    }.collectFirst { case Some(regs) => regs }
  }

  private def cancelLater(batch: TransferBatch): Unit =
    TransferCoordinator.sqlPool.submit(new Runnable {
      override def run(): Unit = batch.cancel()
    })

  private def execute(batch: TransferBatch, regs: Seq[Registration]): Unit = {
    val start = System.currentTimeMillis()
    var sqlRows: Option[Long] = None
    var failure: Option[String] =
      try {
        sqlRows = runBatch(batch)
        None
      } catch {
        case e: Throwable =>
          if (lock.synchronized(cancelled.contains(batch.run))) {
            None // cancelled on purpose: nobody needs the rest, or the run is aborted already
          } else {
            logError(s"Transfer session $id: batch of run ${batch.run} (${regs.size} tasks) failed: $e")
            Some(String.valueOf(e.getMessage))
          }
      }
    lock.synchronized {
      if (failure.isEmpty)
        failure = checkRowsLocked(batch, regs, sqlRows)
      running.remove(batch.run)
      lastActivityMs = System.currentTimeMillis()
      failure match {
        case None if regs.exists(_.state.isInstanceOf[Aborted]) =>
          logInfo(s"Transfer session $id: batch of the aborted run ${batch.run} ended, " +
            s"${System.currentTimeMillis() - start} ms")
        case None =>
          regs.foreach(r => if (!r.state.terminal) r.state = Done)
          logInfo(s"Transfer session $id: batch of run ${batch.run}, epoch ${batch.epoch}: ${regs.size} tasks, " +
            s"${sqlRows.map(n => s"$n rows").getOrElse("rows not counted")}, ${System.currentTimeMillis() - start} ms")
        case Some(reason) =>
          failLocked(batch.run, reason)
      }
      cancelled -= batch.run
      lock.notifyAll()
      scheduleLocked()
    }
  }

  /** Waits for the row reports of the batch tasks and compares them with what Cloudberry transferred */
  private def checkRowsLocked(batch: TransferBatch, regs: Seq[Registration], sqlRows: Option[Long]): Option[String] = {
    if (readTransfer) { // readers: drain your buffers and report
      regs.foreach(r => if (!r.state.terminal) r.state = Transferred)
      lock.notifyAll()
    }
    val deadline = System.currentTimeMillis() + reportTimeoutMs
    def failed = aborted.nonEmpty || closed || regs.exists(_.state.isInstanceOf[Aborted])
    while (!failed && regs.exists(_.rows.isEmpty) && System.currentTimeMillis() < deadline)
      lock.wait(100)
    if (failed)
      None // the abort reason is set already
    else if (regs.exists(_.rows.isEmpty))
      Some(s"${regs.count(_.rows.isEmpty)} of ${regs.size} tasks of run ${batch.run} did not report their rows " +
        s"in $reportTimeoutMs ms")
    else {
      val reported = regs.flatMap(_.rows)
      sqlRows.filter(_ => !reported.contains(-1L) && !cancelled.contains(batch.run)).collect {
        case n if n != reported.sum =>
          s"row count mismatch in run ${batch.run}: Cloudberry ${if (readTransfer) "sent" else "inserted"} $n rows, " +
            s"the ${regs.size} tasks ${if (readTransfer) "received" else "sent"} ${reported.sum}"
      }
    }
  }

  /** A failure aborts the run (a read; a staging write — isolatedRuns: every task attempt is a run) or the session */
  private def failLocked(run: String, reason: String): Unit =
    if (readTransfer || isolatedRuns) abortRunLocked(run, reason) else abortLocked(reason)

  private def abortRunLocked(run: String, reason: String): Unit = {
    logError(s"Transfer session $id: run $run aborted: $reason")
    registrations.valuesIterator.filter(r => r.key.run == run && !r.state.terminal).foreach(_.state = Aborted(reason))
    registrations.valuesIterator.filter(_.key.run == run).foreach(_.batched = true) // no batch for this run
    // its running SQL would go on pushing rows to tasks that are gone: cancel it
    running.get(run).foreach { case (batch, _) => if (cancelled.add(run)) cancelLater(batch) }
    lock.notifyAll()
  }

  private def abortLocked(reason: String): Unit = {
    if (aborted.isEmpty) {
      logError(s"Transfer session $id aborted: $reason")
      aborted = Some(reason)
    }
    registrations.valuesIterator.filter(!_.state.terminal).foreach(_.state = Aborted(reason))
    lock.notifyAll()
  }
}

package com.tender.cloudberry.connector.transfer

/**
 * A batch for the driver SQL: run, epoch, the gpfdist URLs of its tasks and their partition numbers. The
 * SQL registers a canceller of its statement (onCancel) — a read batch whose readers have all stopped
 * early, or of an aborted run, is cancelled.
 */
case class TransferBatch(run: String, epoch: Long, urls: Seq[String], parts: Seq[Int] = Seq.empty) {
  @volatile private var canceller: () => Unit = null

  def onCancel(f: () => Unit): Unit = canceller = f

  private[transfer] def cancel(): Unit = Option(canceller).foreach(f => scala.util.Try(f()))
}

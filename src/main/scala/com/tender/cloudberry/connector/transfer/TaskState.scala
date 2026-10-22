package com.tender.cloudberry.connector.transfer

sealed trait TaskState {
  def render: String
  def terminal: Boolean = true
}

object TaskState {
  case object Pending extends TaskState {
    override val render = "pending"
    override val terminal = false
  }

  /** Read: the INSERT has pushed everything — drain the buffer, report the rows, wait for the check */
  case object Transferred extends TaskState {
    override val render = "transferred"
    override val terminal = false
  }

  case object Done extends TaskState {
    override val render = "done"
  }

  case class Aborted(reason: String) extends TaskState {
    override def render = s"aborted $reason"
  }

  def parse(s: String): TaskState =
    if (s == "done") Done
    else if (s == "transferred") Transferred
    else if (s.startsWith("aborted")) Aborted(s.stripPrefix("aborted").trim)
    else Pending
}

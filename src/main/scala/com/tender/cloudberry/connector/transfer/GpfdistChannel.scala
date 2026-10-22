package com.tender.cloudberry.connector.transfer

import java.nio.ByteBuffer

/**
 * tender: the data side of a task, used by [[GpfdistServer]] (the RMISlave methods before).
 */
trait GpfdistChannel {
  /** GET (Cloudberry reads — a write transfer): the next chunk to send, null — end of data */
  def nextChunk(): ByteBuffer

  def recycle(buff: ByteBuffer): Unit

  /** POST (Cloudberry writes — a read transfer): complete rows of a segment; may block (backpressure) */
  def received(data: Array[Byte]): Unit

  /** POST: a segment has sent all its data */
  def segmentDone(): Unit

  def isAborted: Boolean

  def abortTransfer(reason: String): Unit
}

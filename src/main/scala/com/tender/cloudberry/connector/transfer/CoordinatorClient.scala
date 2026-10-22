package com.tender.cloudberry.connector.transfer

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets

/**
 * tender: executor side client of the [[TransferCoordinator]] (plain HTTP to the driver).
 *
 * @param address host:port of the coordinator
 */
class CoordinatorClient(address: String, sessionId: String, key: TaskKey, epoch: Long, runSize: Int) {
  private def query(extra: (String, String)*): String =
    (Seq("s" -> sessionId, "r" -> key.run, "e" -> epoch.toString, "p" -> key.part.toString,
      "a" -> key.attempt.toString, "n" -> runSize.toString) ++ extra)
      .map { case (k, v) => s"$k=${URLEncoder.encode(v, "UTF-8")}" }.mkString("&")

  /** Registers the gpfdist URL of the task; throws if the coordinator refuses (aborted transfer) */
  def register(gpfdistUrl: String): Unit = {
    val (code, body) = call("POST", s"/register?${query()}", gpfdistUrl, 30000)
    if (code != 200)
      throw new IllegalStateException(s"Transfer coordinator $address refused task ${key.part}: $body")
  }

  /** State of the task batch, waits up to waitMs for a change from `seen` */
  def status(waitMs: Long, seen: TaskState): TaskState = {
    val (code, body) = call("GET", s"/status?${query("wait" -> waitMs.toString, "seen" -> seen.render)}", null,
      waitMs + 30000)
    if (code == 200 || code == 410) TaskState.parse(body)
    else throw new IllegalStateException(s"Transfer coordinator $address: HTTP $code $body")
  }

  /** A write task without rows: done, without a gpfdist server */
  def registerEmpty(): Unit = {
    val (code, body) = call("POST", s"/empty?${query()}", "", 30000)
    if (code != 200)
      throw new IllegalStateException(s"Transfer coordinator $address refused empty task ${key.part}: $body")
  }

  /** The rows the task has sent (write) / received (read); -1 — unknown */
  def finish(rows: Long): Unit = {
    val (code, body) = call("POST", s"/finish?${query("rows" -> rows.toString)}", "", 30000)
    if (code != 200)
      throw new IllegalStateException(s"Transfer coordinator $address: HTTP $code $body")
  }

  def abort(reason: String): Unit = call("POST", s"/abort?${query()}", reason, 30000)

  private def call(method: String, path: String, body: String, timeoutMs: Long): (Int, String) = {
    val conn = new URL(s"http://$address$path").openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod(method)
      conn.setConnectTimeout(10000)
      conn.setReadTimeout(math.min(Int.MaxValue.toLong, timeoutMs).toInt)
      if (body != null) {
        conn.setDoOutput(true)
        val os = conn.getOutputStream
        os.write(body.getBytes(StandardCharsets.UTF_8))
        os.close()
      }
      val code = conn.getResponseCode
      val is = if (code < 400) conn.getInputStream else conn.getErrorStream
      (code, if (is == null) "" else readAll(is))
    } finally {
      conn.disconnect()
    }
  }

  private def readAll(is: InputStream): String = {
    val out = new ByteArrayOutputStream()
    val buff = new Array[Byte](4096)
    var n = is.read(buff)
    while (n > 0) {
      out.write(buff, 0, n)
      n = is.read(buff)
    }
    is.close()
    new String(out.toByteArray, StandardCharsets.UTF_8)
  }
}

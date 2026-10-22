package com.tender.cloudberry.connector.transfer

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

import java.io.ByteArrayOutputStream
import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, ThreadFactory, TimeUnit}
import scala.collection.JavaConverters._

/**
 * tender: driver side control plane of the gpfdist transfer (replaces the RMI master and registry).
 *
 * One HTTP server per driver JVM, a [[TransferSession]] per read scan / write in it. Tasks only call the
 * driver — register the URL of their own gpfdist server, long-poll the outcome of their batch, report an
 * abort: no callbacks into executors and no executor-to-executor traffic, one port to open on the driver
 * (option coordinator.port, 0 = ephemeral).
 *
 * HTTP API, parameters in the query string: s — session, r — run, e — epoch, p — partition,
 * a — attempt, n — partitions of the run:
 *   POST /register (body: gpfdist URL) -> "ok"
 *   GET  /status?wait=ms               -> "pending" | "transferred" | "done" | "aborted <reason>"
 *   POST /finish?rows=N                -> "ok"  (rows the task has sent / received, -1 — unknown)
 *   POST /empty                        -> "ok"  (a write task without rows: done, no gpfdist, no location)
 *   POST /abort (body: reason)         -> "ok"
 */
object TransferCoordinator extends Logging {
  private val sessions = new ConcurrentHashMap[String, TransferSession]()
  private var server: HttpServer = null
  private var advertised: String = null
  /** Batch SQL of the sessions runs here; one batch at a time per session (TransferSession.running) */
  private[transfer] val sqlPool: ExecutorService = Executors.newCachedThreadPool(daemonThreads("cb-transfer-sql"))

  /** host:port of the coordinator for the tasks; the server starts with the first session */
  def address(port: Int): String = synchronized {
    if (server == null)
      start(port)
    advertised
  }

  def register(session: TransferSession): Unit = sessions.put(session.id, session)

  def unregister(id: String): Unit = sessions.remove(id)

  private def start(port: Int): Unit = {
    val httpServer = HttpServer.create(new InetSocketAddress(port), 128)
    httpServer.createContext("/", (exchange: HttpExchange) => handle(exchange))
    httpServer.setExecutor(Executors.newCachedThreadPool(daemonThreads("cb-transfer-coordinator")))
    startAsDaemon(httpServer)
    server = httpServer
    // the address executors reach the driver at (Spark RPC uses it too)
    val host = Option(SparkEnv.get).flatMap(_.conf.getOption("spark.driver.host"))
      .getOrElse(NetUtils().getLocalHostNameAndIp._2)
    advertised = s"$host:${httpServer.getAddress.getPort}"
    logInfo(s"Transfer coordinator is listening on $advertised")
    // time based batch decisions (write waves, read registration timeout), idle sessions cleanup
    val ticker = Executors.newSingleThreadScheduledExecutor(daemonThreads("cb-transfer-ticker"))
    ticker.scheduleWithFixedDelay(() => sessions.values().asScala.foreach { session =>
      try {
        session.tick()
        if (session.idleMs > TransferSession.maxIdleMs) {
          logWarning(s"Transfer session ${session.id} idle for ${session.idleMs} ms, closed")
          session.close("idle session closed")
        }
      } catch {
        case e: Exception => logWarning(s"Transfer session ${session.id} tick failed: $e")
      }
    }, 100, 100, TimeUnit.MILLISECONDS)
  }

  private def handle(exchange: HttpExchange): Unit = {
    var code = 200
    var body = "ok"
    try {
      val q = params(exchange.getRequestURI.getRawQuery)
      Option(sessions.get(q.getOrElse("s", ""))) match {
        case None =>
          code = 410
          body = "aborted unknown or closed transfer session"
        case Some(session) =>
          val key = TaskKey(q("r"), q("p").toInt, q.getOrElse("a", "0").toInt)
          exchange.getRequestURI.getPath match {
            case "/register" =>
              session.register(key, q.getOrElse("e", "0").toLong, q.getOrElse("n", "1").toInt, readBody(exchange))
            case "/status" =>
              body = session.awaitStatus(key, q.getOrElse("wait", "1000").toLong, q.getOrElse("seen", "")).render
            case "/finish" =>
              session.taskFinished(key, q.getOrElse("rows", "-1").toLong)
            case "/empty" =>
              session.registerEmpty(key, q.getOrElse("e", "0").toLong, q.getOrElse("n", "1").toInt)
            case "/abort" =>
              session.taskAborted(key, readBody(exchange))
            case other =>
              code = 404
              body = s"unknown request $other"
          }
      }
    } catch {
      case e: Exception =>
        logWarning(s"Transfer coordinator request ${exchange.getRequestURI} failed: $e")
        code = 500
        body = s"aborted ${e.getMessage}"
    }
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.put("Content-type", List("text/plain; charset=utf-8").asJava)
    exchange.sendResponseHeaders(code, bytes.length)
    val os = exchange.getResponseBody
    os.write(bytes)
    os.close()
  }

  private def params(rawQuery: String): Map[String, String] =
    Option(rawQuery).toSeq.flatMap(_.split("&")).filter(_.nonEmpty).map { kv =>
      val (k, v) = kv.span(_ != '=')
      URLDecoder.decode(k, "UTF-8") -> URLDecoder.decode(v.drop(1), "UTF-8")
    }.toMap

  private def readBody(exchange: HttpExchange): String = {
    val is = exchange.getRequestBody
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

  /**
   * Starts the server from a daemon thread: its dispatcher thread inherits the flag — a non-daemon one
   * would keep the JVM (a spark-shell, a driver) alive after the application ends
   */
  private[transfer] def startAsDaemon(httpServer: HttpServer): Unit = {
    var failure: Throwable = null
    val starter = new Thread(new Runnable {
      override def run(): Unit = try httpServer.start() catch {
        case e: Throwable => failure = e
      }
    }, "cb-transfer-server-start")
    starter.setDaemon(true)
    starter.start()
    starter.join()
    if (failure != null)
      throw failure
  }

  private[transfer] def daemonThreads(prefix: String): ThreadFactory = new ThreadFactory {
    private val counter = new AtomicInteger(0)
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, s"$prefix-${counter.incrementAndGet()}")
      t.setDaemon(true)
      t
    }
  }
}

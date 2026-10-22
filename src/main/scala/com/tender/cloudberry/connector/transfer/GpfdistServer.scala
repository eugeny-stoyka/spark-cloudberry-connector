package com.tender.cloudberry.connector.transfer

import com.tender.cloudberry.connector.utils.ProgressTracker
import org.apache.spark.internal.Logging

import com.github.luben.zstd.{Zstd, ZstdInputStream}

import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.{BindException, InetSocketAddress, ServerSocket, Socket, SocketException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.util.Try

/**
 * Implements GPFDIST protocol natively in scala.
 *
 * tender: one server per task (no service provider shared by the tasks of a host), bound to the first free
 * port of portRange ((0, 0) — an ephemeral one). HTTP over plain sockets, as gpfdist itself: every request of
 * a segment is a connection of its own (Connection: close), the answer is HTTP/1.0, a GET body ends with the
 * connection. Before: com.sun.net.httpserver — it normalizes the header names (X-GP-ZSTD -> X-gp-zstd), while
 * Cloudberry compares them case-sensitively, so a segment could not be told to compress.
 *
 * @param channel  to communicate data with Spark
 * @param compress tender: server.compress — the segments are asked to send zstd frames (a read transfer)
 */
class GpfdistServer(portRange: (Int, Int), channel: GpfdistChannel, metrics: WebServerMetrics,
                    compress: Boolean = false)
  extends Logging
{

  private val executor = Executors.newFixedThreadPool(10, TransferCoordinator.daemonThreads("gpfdist"))
  private val serverSocket: ServerSocket = bind()
  /** tender: the streams of the segments already told to compress (X-GP-ZSTD in their first answer, writeHead) */
  private val compressingStreams = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
  /** tender: server.compress — the zstd frames received, their bytes, the bytes of the rows unpacked from them */
  private val zstdFrames = new java.util.concurrent.atomic.AtomicLong()
  private val zstdBytes = new java.util.concurrent.atomic.AtomicLong()
  private val zstdRowBytes = new java.util.concurrent.atomic.AtomicLong()
  @volatile private var stopped = false

  private def bind(): ServerSocket = {
    val (from, to) = portRange
    var port = from
    var bound: ServerSocket = null
    while (bound == null) {
      try {
        val socket = new ServerSocket()
        try socket.bind(new InetSocketAddress(port), 128) catch {
          case e: Exception =>
            socket.close()
            throw e
        }
        bound = socket
      } catch {
        // tender: a SocketException too — with SO_REUSEADDR (on by default on Linux) two tasks of a JVM can bind
        // the same port at once, the second one fails in listen ("Address already in use (Listen failed)")
        case _: SocketException if from > 0 && port < to => port += 1
        case e: SocketException if from > 0 =>
          throw new BindException(s"No free gpfdist port in the server.port range $from-$to: ${e.getMessage}")
      }
    }
    bound
  }

  def httpPort: Int = serverSocket.getLocalPort

  val progressTracker: ProgressTracker = new ProgressTracker()
  private val webLoopMs = metrics.webLoopMs
  private val processingMs = metrics.processingMs
  private val totalBytes = metrics.totalBytes

  private val segmentCount: AtomicInteger = new AtomicInteger(0)
  private val segmentDoneCount: AtomicInteger = new AtomicInteger(0)

  def nSegInProgress: Int = segmentCount.get() - segmentDoneCount.get()

  private val me = new Object

  // Guarded vars:
  private val activeWriters = MutableHashMap[String, Int]()
  private val segmentQueues = MutableHashMap[String, Array[Byte]]()
  private val segPass = MutableHashMap[String, Int]()

  def stop(): Unit = {
    stopped = true
    Try(serverSocket.close())
    if (compress)
      logInfo(f"server.compress: ${zstdFrames.get} zstd frames, ${zstdBytes.get / 1e6}%.1f MB received for " +
        f"${zstdRowBytes.get / 1e6}%.1f MB of rows")
    executor.shutdownNow()
    me.synchronized {
      segmentQueues.clear()
      activeWriters.clear()
    }
  }

  // ---------- tender: HTTP ----------

  /** A request of a segment: the method, the headers (names case-insensitive), the body stream */
  private case class Request(method: String, headers: java.util.TreeMap[String, String], in: InputStream) {
    def header(name: String): Option[String] = Option(headers.get(name))
  }

  /** A line of the request head (ISO-8859-1, CRLF or LF), null — the connection closed */
  private def readLine(in: InputStream): String = {
    val line = new ByteArrayOutputStream(128)
    var b = in.read()
    if (b < 0)
      return null
    while (b >= 0 && b != '\n') {
      if (b != '\r')
        line.write(b)
      b = in.read()
    }
    new String(line.toByteArray, StandardCharsets.ISO_8859_1)
  }

  private def readRequest(in: InputStream): Request = {
    val requestLine = readLine(in)
    if (requestLine == null || requestLine.isEmpty)
      return null
    val headers = new java.util.TreeMap[String, String](String.CASE_INSENSITIVE_ORDER)
    var line = readLine(in)
    while (line != null && line.nonEmpty) {
      val colon = line.indexOf(':')
      if (colon > 0)
        headers.put(line.substring(0, colon).trim, line.substring(colon + 1).trim)
      line = readLine(in)
    }
    Request(requestLine.split(' ')(0).toUpperCase, headers, in)
  }

  /**
   * The answer head. The header names as com.sun.net.httpserver wrote them before (Cloudberry does not recognize
   * them in this case, as before — e.g. no X-GP-PROTO: the GET body is plain data); X-GP-ZSTD — exactly, Cloudberry
   * recognizes it: server.compress, a POST of a segment that offers zstd (X-GP-ZSTD: 1 in its every request) —
   * after this answer to its first POST the segment sends every body as a zstd frame.
   *
   * Only in the first answer of a stream (segment, transaction, command, scan): Cloudberry 2.1 and Greenplum 7
   * (header_callback of url_curl.c, the same code) create a new zstd context and palloc a new buffer of
   * writable_external_table_bufsize (1 MB) at every answer with X-GP-ZSTD, the old ones stay until the end of
   * the query — before, every POST answered so, a segment held ~1 MB of VMEM per 1 MB sent and a large read
   * failed ("Canceling query because of high VMEM usage", 51.7 GB read from Greenplum 7.1). The flag of the
   * stream stays set without the header.
   */
  private def writeHead(out: OutputStream, request: Request, code: Int, proto: String): Unit = {
    val head = new StringBuilder(s"HTTP/1.0 $code ${if (code == 200) "ok" else "error"}\r\n")
    Seq("Content-type" -> "text/plain", "Expires" -> "0",
      "X-gpfdist-version" -> "6.8.1 build commit:7118e8aca825b743dd9477d19406fcc06fa53852",
      "X-gp-proto" -> proto, "Cache-control" -> "no-cache", "Connection" -> "close").foreach {
      case (name, value) => head.append(name).append(": ").append(value).append("\r\n")
    }
    if (compress && proto == "0" && request.header("X-GP-ZSTD").contains("1") &&
      compressingStreams.add(Seq("X-GP-SEGMENT-ID", "X-GP-XID", "X-GP-CID", "X-GP-SN").map(request.header(_).getOrElse("")).mkString("/")))
      head.append("X-GP-ZSTD: 1\r\n")
    head.append("\r\n")
    out.write(head.toString.getBytes(StandardCharsets.ISO_8859_1))
  }

  private def serve(socket: Socket): Unit = {
    var request: Request = null
    try {
      socket.setTcpNoDelay(true)
      val in = new BufferedInputStream(socket.getInputStream, 65536)
      val out = new BufferedOutputStream(socket.getOutputStream, 65536)
      request = readRequest(in)
      if (request != null) {
        if (request.header("Expect").exists(_.equalsIgnoreCase("100-continue"))) {
          out.write("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1))
          out.flush()
        }
        request.method match {
          case "GET" => get(request, out)
          case "POST" => post(request, out)
          case other =>
            logWarning(s"gpfdist: unsupported method $other from ${socket.getRemoteSocketAddress}")
            writeHead(out, request, 400, "0")
        }
        out.flush()
      }
    } catch {
      case e: Throwable =>
        if (!channel.isAborted && !stopped) {
          val method = Option(request).map(_.method).getOrElse("request")
          logError(s"gpfdist $method from ${socket.getRemoteSocketAddress} failed: $e")
          channel.abortTransfer(s"gpfdist $method failed: ${e.getMessage}")
        }
    } finally {
      Try(socket.close())
    }
  }

  private val acceptor = new Thread(new Runnable {
    override def run(): Unit = {
      while (!stopped) {
        try {
          val socket = serverSocket.accept()
          executor.submit(new Runnable {
            override def run(): Unit = serve(socket)
          })
        } catch {
          case _: SocketException if stopped => // closed by stop()
          case e: Exception => if (!stopped) logWarning(s"gpfdist accept on port $httpPort: $e")
        }
      }
    }
  }, s"gpfdist-accept-$httpPort")
  acceptor.setDaemon(true)
  acceptor.start()

  /** tender: a body of a compressing segment — one zstd frame (the magic 28 b5 2f fd at its start) */
  private def isZstdFrame(b: Array[Byte]): Boolean =
    b.length >= 4 && b(0) == 0x28 && b(1) == 0xb5.toByte && b(2) == 0x2f && b(3) == 0xfd.toByte

  private def unzstd(frame: Array[Byte]): Array[Byte] = {
    val size = Zstd.decompressedSize(frame) // in the frame header (zstd-jni of Spark 3.5); 0 — not there
    if (size > 0 && size <= Int.MaxValue)
      Zstd.decompress(frame, size.toInt)
    else {
      val in = new ZstdInputStream(new ByteArrayInputStream(frame))
      try in.readAllBytes() finally in.close()
    }
  }

  // ---------- gpfdist ----------

  /** Cloudberry reads the data of a write transfer (readable external table) */
  private def get(request: Request, os: OutputStream): Unit = {
    val segmentId = request.header("X-GP-SEGMENT-ID").getOrElse(throw new Exception("No X-GP-SEGMENT-ID"))
    writeHead(os, request, 200, "1")
    logDebug(s"GET for Cloudberry segment $segmentId enter")
    val start = System.currentTimeMillis()
    var nBytes: Long = 0
    var nChunks: Long = 0
    var data = progressTracker.trackProgress("bufferGetMs") {
      channel.nextChunk()
    }
    while ((data != null) && (data.position() > 0)) {
      nChunks += 1
      nBytes += data.position()
      progressTracker.trackProgress("webWriteMs") {
        os.write(data.array(), 0, data.position())
      }
      progressTracker.trackProgress("bufferGetMs") {
        channel.recycle(data)
        data = channel.nextChunk()
      }
    }
    progressTracker.trackProgress("webWriteMs") {
      os.flush()
    }
    val ms = System.currentTimeMillis() - start
    processingMs.addAndGet(ms)
    totalBytes.addAndGet(nBytes)
    webLoopMs.set(progressTracker.results.getOrElse("webWriteMs", 0))
    logDebug(s"GET for Cloudberry segment $segmentId complete in ${ms}ms, nBytes=$nBytes in $nChunks pieces, " +
      s"${progressTracker.reportTimeTaken()}")
  }

  /** Cloudberry writes the data of a read transfer (writable external table) */
  private def post(request: Request, os: OutputStream): Unit = me.synchronized {
    val passStart = System.currentTimeMillis()
    val segmentId = request.header("X-GP-SEGMENT-ID").getOrElse(throw new Exception("No X-GP-SEGMENT-ID"))
    val contentLength = request.header("Content-Length").map(_.toLong).getOrElse(0L) match {
      case n if n >= 0 && n <= Int.MaxValue => n.toInt
      // tender: a Cloudberry 2.1 segment (as Greenplum 7.1) sends a zstd error code (e.g. 4294967226 = -70,
      // dstSize_tooSmall) as the length of a buffer zstd did not shrink (very small or incompressible data),
      // without the rows
      case n if compress => throw new IllegalStateException(s"Cloudberry segment $segmentId sent Content-Length $n — a " +
        "zstd error instead of a compressed buffer (a buffer zstd does not shrink: a few short rows or incompressible " +
        "data); read without server.compress")
      case n => throw new IllegalStateException(s"Cloudberry segment $segmentId sent an invalid Content-Length $n")
    }
    val gpSeq = request.header("X-GP-SEQ").map(_.toInt).getOrElse(0)
    val segmentDone = request.header("X-GP-DONE").map(_.toInt).getOrElse(0)
    if (gpSeq == 1) {
      segmentCount.addAndGet(1)
      logDebug(s"POST: starting download for segmentId=$segmentId, segmentCount=${segmentCount.get()}, " +
        s"X-gp-segment-count=${request.header("X-GP-SEGMENT-COUNT").getOrElse("")}")
    }
    if (!activeWriters.contains(segmentId)) {
      activeWriters.put(segmentId, segmentDone)
      segmentQueues.put(segmentId, Array.empty[Byte])
    }
    segPass.put(segmentId, segPass.getOrElse(segmentId, 0) + 1)
    var bytesReadTotal: Int = 0
    val is = request.in
    if (!channel.isAborted) {
      val body = new Array[Byte](contentLength)
      var nRead = if (contentLength > 0) progressTracker.trackProgress("webReadMs") {
        is.read(body, 0, contentLength)
      } else -1
      while ((nRead > 0) && !channel.isAborted) {
        bytesReadTotal += nRead
        nRead = if (bytesReadTotal < contentLength) progressTracker.trackProgress("webReadMs") {
          is.read(body, bytesReadTotal, contentLength - bytesReadTotal)
        } else -1
      }
      if (!channel.isAborted) {
        // tender: a zstd frame of a compressing segment (server.compress), the rows as they are otherwise
        val data = if (compress && isZstdFrame(body)) {
          val rows = progressTracker.trackProgress("unzstdMs")(unzstd(body))
          zstdFrames.incrementAndGet(); zstdBytes.addAndGet(body.length); zstdRowBytes.addAndGet(rows.length)
          rows
        } else body
        // the unterminated tail of the previous request of this segment + this body
        val prev = segmentQueues(segmentId)
        val buff = if (prev.isEmpty) data else prev ++ data
        val buffLen = buff.length
        val eolIx = buff.lastIndexOf(0x0a)
        var tail: Array[Byte] = Array.empty[Byte]
        if (eolIx >= 0) {
          if (eolIx < buffLen - 1) {
            val len = buffLen - eolIx - 1
            tail = Array.ofDim[Byte](len)
            Array.copy(buff, eolIx + 1, tail, 0, len)
          }
          progressTracker.trackProgress("bufferPutMs") {
            channel.received(buff.slice(0, eolIx + 1))
          }
        } else {
          tail = buff
        }
        if (tail.nonEmpty && segmentDone != 0) {
          logError(s"Truncated row received in Cloudberry segment $segmentId")
          channel.abortTransfer(s"truncated row received from Cloudberry segment $segmentId")
        }
        segmentQueues.put(segmentId, tail)
      }
    }
    writeHead(os, request, if (!channel.isAborted) 200 else 500, "0")
    os.flush()
    processingMs.addAndGet(System.currentTimeMillis() - passStart)
    totalBytes.addAndGet(bytesReadTotal)
    if (segmentDone != 0) {
      segmentDoneCount.addAndGet(1)
      if (!channel.isAborted)
        channel.segmentDone()
      logDebug(s"POST: segment transfer complete for segmentId=$segmentId, gpSeq=$gpSeq, " +
        s"all segments: started=${segmentCount.get()}, complete=${segmentDoneCount.get()}, " +
        s"totalBytes=${totalBytes.get()}, processingMs=${processingMs.get()}")
    }
    webLoopMs.set(progressTracker.results.getOrElse("webReadMs", 0))
  }
}

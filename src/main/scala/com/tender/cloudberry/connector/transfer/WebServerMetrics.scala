package com.tender.cloudberry.connector.transfer

import java.util.concurrent.atomic.AtomicLong

class WebServerMetrics {
  val webLoopMs = new AtomicLong(0)
  val processingMs = new AtomicLong(0)
  val totalBytes = new AtomicLong(0)
}

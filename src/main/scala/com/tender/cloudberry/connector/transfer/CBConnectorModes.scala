package com.tender.cloudberry.connector.transfer

object CBConnectorModes extends Enumeration {
  type CBConnectorMode = Value
  val Batch, MicroBatch, Continuous = Value
}

package com.tender.cloudberry.connector.writer

final case class CloudberryConnectionExpired(private val message: String = "",
                                      private val cause: Throwable = None.orNull) extends Exception(message, cause)

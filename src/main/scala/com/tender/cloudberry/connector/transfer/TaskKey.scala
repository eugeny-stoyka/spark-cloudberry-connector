package com.tender.cloudberry.connector.transfer

/** A task of a transfer: run (a stage execution / an epoch) + partition + attempt */
case class TaskKey(run: String, part: Int, attempt: Int)

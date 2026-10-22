package com.tender.cloudberry.connector.transfer

import com.tender.cloudberry.connector.transfer.TaskState.Pending

/**
 * A registered task: its gpfdist URL, state, whether it is already in a batch, and the rows it has
 * reported (sent for a write, received for a read; -1 — unknown, e.g. a reader stopped early)
 */
private[transfer] case class Registration(key: TaskKey, epoch: Long, url: String, var batched: Boolean = false,
                                          var state: TaskState = Pending, var rows: Option[Long] = None)

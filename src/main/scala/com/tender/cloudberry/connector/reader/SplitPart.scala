package com.tender.cloudberry.connector.reader

/**
 * tender: a part of a split read (read.split): its rows are `SELECT ... FROM source [WHERE condition]` of every
 * source, UNION ALL — leaf partitions (partitions), the table with gp_segment_id of its segments (segments),
 * the table with a range of a column (column)
 *
 * @param locations hosts that hold the rows of the part (the segments of a segments split), for Spark locality
 */
private[reader] case class SplitPart(sources: Seq[String], condition: String = "", locations: Array[String] = Array.empty)

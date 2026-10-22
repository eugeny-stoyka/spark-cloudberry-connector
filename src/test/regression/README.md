# Regression suite (branch spark-3.5.8)

End-to-end checks of the connector on the docker stand of this repository ([docker/](../../../docker/README.md)):
Spark 3.5.8 (local and YARN, client mode) and Cloudberry 2.1.0-incubating with `CLOUDBERRY_SEGMENTS` primary segments (4). Every run is a
`spark-shell` in the `client` container of the stand fed with a probe from [probes/](probes/); the
connector jar and the PostgreSQL JDBC driver are passed with `--jars`, as an application does.

## Run

From the root of this repository (Git Bash on Windows or Linux):

```bash
docker/run.sh jar                     # target/spark-cloudberry-connector_2.12-1.0.jar (or BUILD.md)
docker/run.sh cluster up              # the stand: HDFS, YARN, Spark, Cloudberry
docker/run.sh regression              # all runs (= src/test/regression/run.sh)
docker/run.sh regression stream       # runs whose name contains "stream"
docker/run.sh all                     # the three above
```

Variables: `CONNECTOR_JAR` — another connector jar, `CB_SCHEMA` — schema of the test tables
(`test`, created by the stand), `PG_DRIVER_JAR` — PostgreSQL driver inside the client
container (default: the one of the stand image, `/opt/jars/postgresql-42.7.4.jar`).

The runs go one after another (they share the test tables, parallel runs would interfere). The suite
creates its source tables — `its_payments` (10 payments 101..110: integer, numeric(12,2), date,
timestamp, text) and the partitioned `its_parts` (100000 rows in 7 monthly leaf partitions, the last
one empty), both analyzed — and drops all its `its_*` tables at the end.

## Runs

| Run | Probe | Mode | Checks |
|-----|-------|------|--------|
| `names-local1` | `names` | `local[1]` | the data source name `cloudberry`: read, overwrite + truncate, read back; reads without the distribution key column (a column subset, a pushed filter on the key); option names in any case (`sqltransfer`, `SQLTRANSFER`) |
| `parser-local1` | `parser` | `local[1]` | the parser of a read (`TextRowParser`, from the bytes) against the parser of the strings before it (`SparkSchemaUtil.textToInternalRow`): 50000 generated lines of the TEXT format in each of 4 DB time zones (UTC, Europe/Moscow, America/New_York, +05:30) — escapes, NULL and `"\N"` as a value, the edges of the integers, doubles, booleans, dates of the years 1..9999, timestamps with 0..6 digits of the fraction and the DST gaps / overlaps, decimals to round, arrays, bytea, a field too few / too many; the same values or the same failure, the next line found |
| `writer-local1` | `writer` | `local[1]` | the writer of a write (`TextRowWriter`, the rows into bytes, in blocks) against the String of a row before it (`SparkSchemaUtil.internalRowToText`) + LF in UTF-8: 20000 random rows of all the types in each of 4 DB time zones — strings with escapes, emoji and broken UTF-8, the edges of the integers, NaN / -0.0, dates and timestamps also out of the years 1..9999 and at the DST changes, decimals written with an exponent, arrays, maps, binary, NULL — as `UnsafeRow` and `GenericInternalRow`; the same bytes row by row and for a block of 1000 rows |
| `types-local2` | `types` | `local[2]` | strings (non-ASCII, TEXT escapes, an empty string and `"NULL"` vs NULL), a date after 2038, timestamp microseconds; arrays of scalars as Cloudberry arrays, maps / structs / nested arrays as JSONB — the column types, read back with the schema; existing `int[]` / `text[]` / `jsonb` read with the inferred and a given schema; arrays / structs into `jsonb` / `text` columns; strings read with `server.compress`; `bit(1)`, `bit(8)`, `time`, `interval`, `inet`, `money`, `point`, `uuid[]`, `jsonb[]`, `bytea[]` read and written back, `timetz` read; `smallint` / `tinyint` / `timestamp_ntz` written, `timestamp_ntz` read with a schema and a pushed filter |
| `write-local2`, `write-local4` | `write` | `local[2]`, `local[4]` | a DataFrame built in Spark in 2 / 4 partitions: overwrite, overwrite + truncate, append from 1 partition, `sum` after append; a new table with `distributedby` + `partitionclause`, an overwrite with `distributedby replicated`; `storageclause`: a PAX table (overwrite, append, read back), a partitioned AO column one; read / write with a `server.port` range |
| `nested-local2` | `nested` | `local[2]` | a DataFrame read from Cloudberry written back into Cloudberry (read nested in a write): overwrite with and without truncate, the copy equals the source (`exceptAll`) |
| `write-yarn-1x2`, `write-yarn-2x2` | `write` | YARN, 1 x 2 and 2 x 2 executors x cores | the same as `write-local*` with executors on YARN |
| `parallelism-yarn-2x2`, `parallelism-yarn-1x2` | `parallelism` | YARN 2 x 2 and 1 x 2 | read partitions without `parallelism` (= segments; on 1 x 2 — by the task slots, not the executors) and with `parallelism=1`; write tasks with `parallelism=2` / `1`, with `write.repartition=false` (= input partitions); `parallelism=-1` is rejected |
| `stats-local2` | `stats` | `local[2]` | optimizer statistics of a read: rows and bytes of an analyzed table from the catalog, unknown for a query (`spark.sql.defaultSizeInBytes`, no broadcast); a partitioned table — the rows of its leaves (not twice) |
| `speculation-yarn-2x2` | `speculation` | YARN 2 x 2, `spark.speculation=true` | a read and a write are refused before any transfer; a split read (`read.split`) and a staging write (`write.staging`) are allowed |
| `split-local2` | `split` | `local[2,2]` (one task retry) | `read.split=partitions` on `its_parts` (7 monthly leaf partitions): partitions = leaves, `read.split.parts=3`, the same rows as the classic read, a pushed filter, `limit 5`, not applied to a query; `segments`: a part per segment, a table without partitions; `column`: ranges of `id` and of the date, a query; a task failing once after 10 rows — its part is read again, the rows are complete (`partitions`, `segments`); one snapshot — rows inserted during the read are not seen (`read.split.snapshot=false` — the later parts see them) |
| `staging-local2` | `staging` | `local[2,2]` (one task retry) | `write.staging=true`: overwrite, append, overwrite + truncate (in the commit); a write task failing once after 1000 rows (`write.repartition=false`) — retried, the rows are complete, no duplicates; no staging table left |
| `backpressure-local4` | `backpressure` | `local[4]` | `buffer.max=2m`: 3 million rows written and read back, count, `sum(id)` and distinct match; read back with `server.compress` |
| `limit-local2` | `limit` | `local[2]` | `limit 5` on 1 million rows (readers closed early, the INSERT cancelled), then a full count and `sum(id)` of the same table |
| `stream-local2`, `stream-yarn-2x2` | `stream` | `local[2]`, YARN 2 x 2 | Structured Streaming read (`offset.select` + `sqlTransfer`): exactly the 3000 rows inserted after the start; write of a `rate` stream: no duplicates, not less than the rows Spark committed |

## Results

`target/regression/<run>.results` — one line per check, `<step> -> <value> [<ms> ms]`, a value
other than expected is marked `MISMATCH`, an exception `FAILED`; the last line is `DONE`.
`<run>.log` — the whole `spark-shell` output. A run fails on a non-zero exit code, a missing `DONE`
(a probe stopped half-way: a compile error of a REPL line, a timeout), `FAILED` / `MISMATCH` in the
results, or errors in the log (`ERROR`, exceptions, `NullPointer`, `Broken pipe`, `Connection reset`,
REPL compile errors); not counted: the expected rejections of `parallelism=-1` and of
`spark.speculation=true`, the checkpoint write Spark interrupts itself on `stop()` of a stream, and the
log lines a run allows (`ALLOW_LOG=<regex>` in `RUNS` — the failure a probe causes on purpose).

A new probe: a file in `probes/` that starts with `:load /regression/probes/common.scala`, uses
`check(step, expected) { ... }` / `checkFails(step, message) { ... }`, ends with `done()` and `:quit`,
plus a line in `RUNS` of `run.sh`.

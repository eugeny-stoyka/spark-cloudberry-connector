# Tender Spark Cloudberry Connector
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Spark 3.5 data source for Apache Cloudberry (DataSource V2): batch reads and writes, Structured Streaming
(micro-batches).

- **Direct transfer.** Rows go between the Cloudberry segments and the Spark tasks over the gpfdist protocol,
  in parallel; JDBC is used only on the driver, for control.
- **Large tables.** Split reads and staging writes repeat only the failed part of a transfer.
- **Fast text.** Read tasks parse the rows straight from the bytes the segments send, write tasks format them
  straight into bytes.
- **Cloudberry storage.** A write can create PAX and append-optimized tables, distributed and partitioned.

Inspired by the open-source Spark connector of **ITSumma** — the connector is reworked here throughout. License:
MIT ([LICENSE](LICENSE)). Packages `com.tender.cloudberry.connector` /
`org.apache.spark.sql.tender.cloudberry.connector`, Maven group `com.tender`, data source name `cloudberry`.
*Tender* is just the name of the author's project work, not an organization.

**Contents:** [Requirements](#requirements) · [Build](#build) · [Quick start](#quick-start) ·
[Examples](#examples) · [How it works](#how-it-works) · [Large tables](#large-tables) · [Options](#options) ·
[Types](#types) · [Performance](#performance) · [Limitations](#limitations) · [Development](#development)

## Requirements

| Component | Version |
|---|---|
| Spark | 3.5 (built for 3.5.8), Scala 2.12 |
| Java | 11 |
| Cloudberry | 2.x (checked on 2.1.0-incubating) |
| JDBC driver | PostgreSQL 42.7.4, not in the jar |

- **Network**: every Cloudberry segment -> every executor host, on the gpfdist ports of the tasks
  (`server.port`); executors -> the driver, on one port (`coordinator.port`).
- **Privileges**: creating gpfdist external tables (readable for a write, writable for a read); `CREATE` in the
  schema of the tables a write creates.

## Build

```bash
mvn clean package        # JDK 11, Maven 3.9
docker/run.sh jar        # or in Docker, without a local JDK / Maven
```

The result is `target/spark-cloudberry-connector_2.12-1.0.jar`. `commons-dbcp2` / `commons-pool2` are relocated
into `com.tender.cloudberry.connector.shaded.*`: no clash with the Kafka connector on the classpath. More in
[BUILD.md](BUILD.md).

## Quick start

```bash
spark-shell --jars spark-cloudberry-connector_2.12-1.0.jar,postgresql-42.7.4.jar
```

The user of the connector:

```sql
create user database_user with password 'yourpassword';
alter role database_user with createexttable (type = 'readable', protocol = 'gpfdist');
alter role database_user with createexttable (type = 'writable', protocol = 'gpfdist');
grant usage, create on schema public to database_user;
```

The connection, for all the examples:

```scala
val cb = Map(
  "url" -> "jdbc:postgresql://cb-coordinator:5432/database",
  "user" -> "database_user", "password" -> "yourpassword", "dbschema" -> "public")
```

Read a table or a query — the selected columns and the filters go to Cloudberry:

```scala
val payments = spark.read.format("cloudberry").options(cb).option("dbtable", "payments").load()
payments.where("amount > 100").select("id", "amount").show()

val large = spark.read.format("cloudberry").options(cb)
  .option("dbtable", "select id, amount from payments where amount > 100").load()
```

Write — the mode is required (`append` or `overwrite`); a missing table is created:

```scala
payments.write.format("cloudberry").options(cb).option("dbtable", "payments_copy").mode("append").save()

// overwrite: DROP and CREATE; with truncate — TRUNCATE, the table definition stays
payments.write.format("cloudberry").options(cb).option("dbtable", "payments_copy")
  .option("truncate", "true").mode("overwrite").save()
```

## Examples

### A new table: storage, distribution, partitions

```scala
payments.write.format("cloudberry").options(cb).option("dbtable", "payments_by_month")
  .option("storageclause", "using pax")
  .option("distributedby", "id")
  .option("partitionclause", "partition by range (d) (start (date '2026-01-01') inclusive " +
    "end (date '2027-01-01') exclusive every (interval '1 month'))")
  .mode("overwrite").save()

// an append-optimized column table, zstd-compressed
payments.write.format("cloudberry").options(cb).option("dbtable", "payments_ao")
  .option("storageclause", "with (appendoptimized=true, orientation=column, compresstype=zstd)")
  .mode("overwrite").save()
```

### Parallelism

```scala
// at most 2 read tasks (by default — min(primary segments, task slots))
spark.read.format("cloudberry").options(cb).option("dbtable", "payments").option("parallelism", "2").load()

// a write task per input partition, no shuffle: for an input that is already even
payments.write.format("cloudberry").options(cb).option("dbtable", "payments_copy")
  .option("write.repartition", "false").mode("append").save()

// the gpfdist ports of the tasks from a range: a firewall between the segments and the executors
spark.read.format("cloudberry").options(cb).option("dbtable", "payments")
  .option("server.port", "45000-45100").load()
```

### Large tables

```scala
// a split read by leaf partitions: a failed task reads only its part again
spark.read.format("cloudberry").options(cb).option("dbtable", "sales")
  .option("read.split", "partitions").load()

// a split read by ranges of a column (a partition key or an indexed column)
spark.read.format("cloudberry").options(cb).option("dbtable", "events")
  .option("read.split", "column").option("read.split.column", "event_date")
  .option("read.split.lowerBound", "2026-01-01").option("read.split.upperBound", "2027-01-01").load()

// a staging write: a failed task writes only its part again; the old rows stay until the commit
sales.write.format("cloudberry").options(cb).option("dbtable", "sales_copy")
  .option("write.staging", "true").option("truncate", "true").mode("overwrite").save()

// the rows of a read zstd-compressed by the segments (large compressible text)
spark.read.format("cloudberry").options(cb).option("dbtable", "logs")
  .option("server.compress", "true").load()
```

### JSON as maps and structs

```scala
import org.apache.spark.sql.types._

// a jsonb column read as a map with the schema given
val schema = StructType(Seq(
  StructField("id", IntegerType),
  StructField("attrs", MapType(StringType, StringType))))
spark.read.schema(schema).format("cloudberry").options(cb).option("dbtable", "products").load()
```

### Your SQL in the transfer

`<ext_table>` in `sqltransfer` is the external table of the transfer: a read inserts into it, a write selects
from it.

```scala
// read: joins, functions in Cloudberry; dbtable gives the schema of the result
spark.read.format("cloudberry").options(cb)
  .option("dbtable", "select id, amount from payments where false")
  .option("sqltransfer", "insert into <ext_table> select p.id, p.amount * r.rate " +
    "from payments p join rates r using (currency)")
  .load()

// write
payments.write.format("cloudberry").options(cb).option("dbtable", "large_payments")
  .option("sqltransfer", "insert into large_payments select * from <ext_table> where amount > 100")
  .mode("append").save()
```

### Structured Streaming

```scala
import org.apache.spark.sql.streaming.Trigger

// read: offset.select gives the latest position of the source (any JSON), sqltransfer reads the rows
// between the start (exclusive) and the end (inclusive) offsets of a micro-batch
val events = spark.readStream.format("cloudberry").options(cb)
  .option("dbtable", "events")
  .option("offset.select", "select json_build_object('max_id', coalesce(max(id), 0))::text from events")
  .option("sqltransfer", "insert into <ext_table> select id, ts from events " +
    "where id > coalesce((nullif('<start_offset_json>', '[null]')::json->>'max_id')::bigint, 0) " +
    "and id <= ('<end_offset_json>'::json->>'max_id')::bigint")
  .load()

// write: a micro-batch — one transaction
events.writeStream.format("cloudberry").options(cb).option("dbtable", "events_copy")
  .option("checkpointLocation", "hdfs:///checkpoints/events_copy")
  .trigger(Trigger.ProcessingTime("10 seconds")).start()
```

A full pipeline: [examples/streaming-example.scala](examples/streaming-example.scala).

## How it works

### The transfer

Every task runs its own gpfdist server (`server.port`) with a bounded buffer (`buffer.max`): a fast side waits
for a slow one. The tasks register with the transfer coordinator on the driver (`coordinator.port`); there is
no traffic between executors.

- **Read**: min(primary segments, task slots, `parallelism`) tasks. When all of them have started, the driver
  runs one `INSERT INTO <writable external table> SELECT ...` — the segments push the rows to the tasks; the
  read sees one snapshot.
- **Write**: the input is repartitioned by the hash of the columns into min(primary segments, task slots) tasks
  (`write.repartition=false` — no shuffle, a task per input partition). The driver runs `INSERT INTO target
  SELECT FROM <readable external table>`, one transaction committed by the job commit: the write is atomic.
- **Row counts are checked**: the rows the tasks sent / received against the rows of the INSERT; a mismatch
  fails the read / rolls the write back.

### The rows

The transfer is the TEXT format of the external tables; the tasks work on its bytes.

- **Read** (`TextRowParser`): a row per `next()` straight from the buffer of the task, the fields and the line
  end found in one pass, one row object for all the rows. Strings are copies of their UTF-8 bytes (escapes undone
  byte by byte); integers, decimals of up to 18 digits, booleans, dates and timestamps (DateStyle ISO) are parsed
  from the bytes.
- **Write** (`TextRowWriter`): the rows go straight into bytes and to the gpfdist server in blocks of ~64 KB.
  Strings are their UTF-8 bytes with the TEXT escapes (checked to be valid UTF-8); integers, booleans, decimals
  of up to 18 digits, dates and timestamps of the years 1..9999, `bytea` (base64) are formatted into the bytes.
- **Anything else** — arrays, JSON, doubles on a write, unusual values (infinity, BC dates, decimals to round)
  — goes through its text, field by field. The regression checks both directions against the text of the
  fields value by value and byte by byte (`parser-local1`, `writer-local1`).

### The rest

- **Statistics**: a read of a table reports its rows and size from the catalog (as of the last `ANALYZE`; a
  partitioned table — the sum of its leaves) to the Spark optimizer — a large table is not broadcast in a join.
- **Failures**: a classic read or write is one transfer — a task restarted after its rows went into it fails
  the read / rolls the write back, so `spark.speculation=true` is refused (outside local mode). For large data
  use a split read and a staging write: they repeat only the failed part.
- **The server**: the version of Cloudberry is logged at the first operation on a URL (INFO); a server without
  "Apache Cloudberry" in `version()` gets a warning. The external tables are `gp_exttable_fdw` of Cloudberry (in
  `pg_catalog` of every database, template0 too); `gp_toolkit` is not needed.

## Large tables

### Split read

`read.split` reads a table in independent parts: a Spark partition per part, every part is its own `INSERT ...
SELECT` on its own connection. A failed task reads only its part again, speculation is allowed. The parts see
one snapshot (`pg_export_snapshot`; `read.split.snapshot=false` — every part its own).

| `read.split` | Parts | For |
|---|---|---|
| `partitions` | groups of leaf partitions | partitioned tables |
| `segments` | a part per primary segment | any table but a replicated one ¹ |
| `column` | ranges of a column ² | a partition key or an indexed column |

1. Only when Cloudberry dispatches `WHERE gp_segment_id = N` to one segment (checked by `EXPLAIN`; Cloudberry
   2.1 does it with GPORCA and with the Postgres planner).
2. `read.split.column` (integral, date, timestamp) between `read.split.lowerBound` and `read.split.upperBound`,
   as `partitionColumn` of Spark JDBC; tables and queries.

When the mode does not fit (a query for `partitions`, a table without partitions, `sqltransfer`, a stream), the
read goes as one transfer, with a warning.

### Staging write

`write.staging=true` (a batch write): every task attempt fills a staging table of its own
`scc_stg_<session>_<partition>_<attempt>` (`LIKE` the target, in its schema; a heap table whatever the storage
of the target). The job commit moves the tables of the committed attempts into the target in one transaction —
an overwrite with `truncate` truncates the target there, so a failed write keeps the old rows — and drops them.
A failed task writes only its part again, speculation is allowed. Needs `CREATE` in the schema of the target.

### Compression of a read

`server.compress=true`: the segments send the rows zstd-compressed (the protocol of `gpfdist --compress`; a
Cloudberry built with zstd, as the image of the stand — without it the rows come plain). Less network, more CPU
on the segments; a write is not compressed.

**Defect of Cloudberry 2.1** (`url_curl.c`): a buffer zstd does not shrink — a few short rows, incompressible
data (UUIDs, hashes, base64) — comes as an error code without its rows. The read then fails ("read without
server.compress"), rows are not lost silently. Use it for large compressible text only. The task logs (INFO)
what came compressed: "zstd frames, MB received for MB of rows".

## Options

### Connection

| Option | Default | Meaning |
|---|---|---|
| `url` | — | JDBC URL of the coordinator, required |
| `user`, `password` | — | |
| `dbschema` | `search_path` | schemas of the tables ¹ |
| `dbtimezone` | JVM time zone | time zone of the database |
| `ApplicationName` | Spark app id | session name in Cloudberry ² |
| `tcpKeepAlive` | `true` | TCP keepalive of JDBC connections ³ |
| `dbmessages` | `OFF` | log level of `RAISE NOTICE` ⁴ |
| `pool.maxSize` | `8` | driver JDBC pool: sessions at most |
| `pool.minIdle` | `0` | idle sessions kept |
| `pool.idleMs` | `60000` | an idle session is closed after |
| `pool.timeoutMs` | `network.timeout` | wait for a free session |

1. A schema or a comma separated list, where to create / search the tables; `public` is added.
2. Case-sensitive.
3. A transfer INSERT has no traffic while it runs: a firewall / NAT may drop an idle connection.
4. `INFO`, `WARN` or `OFF`.

### Source and target

| Option | Default | Meaning |
|---|---|---|
| `dbtable` | — | a table; for a read also a query ¹ |
| `sqltransfer` | — | own SQL instead of the transfer ² |
| `truncate` | `false` | overwrite by `TRUNCATE` ³ |
| `storageclause` | — | `USING` / `WITH` of a created table ⁴ |
| `distributedby` | — | `DISTRIBUTED BY` of a created table |
| `partitionclause` | — | appended to `CREATE TABLE` ⁵ |
| `tempexttables` | `true` | temporary external tables |
| `undo.side.effects.sql` | — | SQL run on an abort ⁶ |

1. Required without `sqltransfer`.
2. SQL or a PL/pgSQL block instead of the `INSERT ... SELECT`; `<ext_table>` — the external table of the
   transfer.
3. Not `DROP` / `CREATE`: keeps the table definition.
4. Before `DISTRIBUTED BY`: `using pax`, `using ao_column`, `with (appendoptimized=true, orientation=column,
   compresstype=zstd)`; without it — the default of the server (heap). Also the storage of the leaves of
   `partitionclause`.
5. After `DISTRIBUTED BY`, e.g. the partitioning of the target of a write (the classic syntax with the
   partitions, as in the example above).
6. A write with `sqltransfer`: undoes its side effects.

### Parallelism and transfer

| Option | Default | Meaning |
|---|---|---|
| `parallelism` | `0` (auto) | read / write tasks ¹ |
| `default_max_read_segments` | `64` | read tasks at most |
| `default_max_write_segments` | `64` | write tasks at most |
| `write.repartition` | `true` | `false` — no shuffle ² |
| `network.timeout` | 60 s ³ | driver — task communication |
| `server.port` | `0` | gpfdist port of a task ⁴ |
| `server.timeout` | unlimited | time limit of a transfer |
| `server.compress` | `false` | zstd compression of a read |
| `coordinator.port` | `0` | coordinator port on the driver ⁵ |
| `buffer.max` | `64m` | queued bytes per task ⁶ |
| `transfer.settleMs` | `1000` | pause closing a write batch ⁷ |

1. Read: at most that many; write: that many. Auto — min(primary segments, task slots); more than primary
   segments is cut, with a warning.
2. A write task per input partition.
3. Half of `spark.network.timeout`; also how long all tasks of a read may take to start.
4. `0` — ephemeral, `N` — that port, `A-B` — the first free one of the range (for a firewall).
5. `0` — ephemeral; the first session of the JVM sets it.
6. `0` — unbounded.
7. Write tasks starting while none comes for that long form one INSERT; normally all start at once.

### Split read and staging write

| Option | Default | Meaning |
|---|---|---|
| `read.split` | `none` | `partitions`, `segments`, `column` |
| `read.split.parts` | `0` (auto) | parts of a split read ¹ |
| `read.split.column` | — | integral, date or timestamp |
| `read.split.lowerBound` | — | range start ² |
| `read.split.upperBound` | — | range end ² |
| `read.split.snapshot` | `true` | one snapshot for the parts |
| `write.staging` | `false` | write through staging tables |

1. `partitions`, `column` — 4 x task slots; `segments` — a part per primary segment.
2. `100`, `2026-01-01`, `2026-01-01 10:00:00`; the first part also takes the values below and NULL, the last —
   above.

### Structured Streaming

| Option | Default | Meaning |
|---|---|---|
| `offset.select` | — | SQL: the latest offset (JSON) ¹ |
| `offset.update` | — | SQL with `?`: the committed offset |
| `stream.read.autocommit` | `true` | `false` — a transaction per micro-batch |

1. Run at the start of every micro-batch; `<start_offset_json>` and `<end_offset_json>` in `sqltransfer` — the
   offsets of the micro-batch (the start exclusive, the end inclusive).

## Types

| Cloudberry | Spark | Note |
|---|---|---|
| `smallint`, `integer`, `bigint` | `smallint`, `int`, `bigint` | ¹ |
| `real`, `double precision` | `float`, `double` | |
| `numeric(p, s)`, `numeric` | `decimal(p, s)`, `decimal(38, 18)` | ² |
| `boolean`, `bit(1)` | `boolean` | |
| `text`, `varchar(n)`, `char(n)` | `string` | ³ |
| `uuid`, `bit(n)`, `varbit` | `string` | |
| `jsonb`, `json` | `string`, `map`, `struct` | ⁴ |
| `bytea` | `binary` | |
| `date`, `timestamp`, `timestamptz` | `date`, `timestamp` | ⁵ |
| `time`, `timetz` | `timestamp` | ⁶ |
| `interval`, `inet`, `money`, ... | `string` | ⁷ |
| arrays of the types above | `array` of them | ⁸ |

In a row the types pair in order: `real` — `float`, `double precision` — `double`. A table a write creates gets
the first Cloudberry type of the Spark type: `string` — `text`, `map` / `struct` — `jsonb`.

1. `tinyint` is written as `smallint`.
2. `numeric` without a precision: the values are kept, written back with the scale 18.
3. Any characters; an empty string and `"NULL"` stay values.
4. A read gives the JSON text; with a schema given (`spark.read.schema(...)`) — a `map`, `struct` or `array`:
   struct fields by name, missing ones are NULL, `jsonb` may reorder the keys. A write creates `jsonb` for a
   `map`, `struct` and arrays of arrays / maps / structs / binary.
5. Microseconds kept; `timestamptz` — the instant (`10:20:30+03` is `07:20:30` UTC). `timestamp_ntz` is written
   as `timestamp` (the local date-time), read so with a schema given.
6. On 1970-01-01, as Spark JDBC reads them; `timetz` — the instant, written back with the offset of the session.
7. Any other type (`point`, `cidr`, `macaddr`, ...): read as its text, written back with a cast.
8. `integer[]`, `text[]`, `uuid[]`, `jsonb[]`, `bytea[]`, ... — `array<int>`, `array<string>`, `array<binary>`,
   ...

## Performance

Measured on the stand of this repository ([docker/](docker/README.md)), all of it on one machine, with the
resource profiles of the stand (`STAND_RESOURCES`): the Cloudberry segments and the Spark task slots grow together,
a slot per segment.

| Component | Stand |
|---|---|
| CPU | Intel Core i9-14900K, 24 cores (8 P + 16 E), 32 threads |
| Memory | 96 GB |
| Disk | Samsung SSD 990 PRO 2 TB, NVMe |
| OS | Windows 11 Pro, Docker Desktop 29.6, WSL 2 |
| Network | the Docker bridge, no physical network |
| Cloudberry | 2.1.0-incubating, single-node: the coordinator and the primary segments in one container |
| HDFS | Hadoop 3.3.6, one DataNode |
| `default` | 4 Cloudberry segments; YARN NodeManager 6 GB, 8 vcores; Spark 2 executors x 2 cores x 2 GB — 4 task slots |
| `large` | 8 Cloudberry segments; YARN NodeManager 20 GB, 16 vcores; Spark 2 executors x 4 cores x 4 GB — 8 task slots |

**The data**: `its_stress` (`docker/run.sh stress generate 50000000`) — 50 million rows of `id bigint, d date,
amount numeric(12,2), note text` (a note of ~70 characters), 12 monthly leaf partitions, distributed by `id`;
4.5 GB as text, 5.5 GB in Cloudberry, 1.2–1.4 GB as Parquet (snappy).

**The run** (`docker/run.sh stress run`, 2026-10-22): an untimed warm-up first, then every scenario once. The
time is of the transfer only; the result is checked after it — the count and the sums of every column of the
Parquet, the rows of the table. The speed is the text size (4.5 GB) / the time.

| What happens | Tasks ¹ | `default`: 4 segments / 4 slots | `large`: 8 / 8 | large / default |
|---|---|---|---|---|
| **Export: Spark reads `its_stress` from Cloudberry and writes it into HDFS as Parquet** | | | | |
| one transfer of the whole table | segments | 6.9 s, 652 MB/s | 5.1 s, 884 MB/s | 1.36 |
| one transfer, the segments compress the rows (`server.compress`) | segments | 6.9 s, 652 MB/s | 4.9 s, 923 MB/s | 1.42 |
| a transfer per group of leaf partitions (`read.split=partitions`) | 12 parts | 9.1 s, 494 MB/s | 6.4 s, 707 MB/s | 1.43 |
| the same, compressed | 12 parts | 8.8 s, 510 MB/s | 6.2 s, 725 MB/s | 1.42 |
| a transfer per segment (`read.split=segments`) | segments | 6.8 s, 664 MB/s | 4.9 s, 915 MB/s | 1.38 |
| **Import: Spark reads that Parquet from HDFS and writes it into a Cloudberry table** | | | | |
| the rows shuffled into the write tasks, one INSERT (the default) | segments | 23.2 s, 195 MB/s | 17.5 s, 258 MB/s | 1.33 |
| no shuffle: a write task per Parquet split (`write.repartition=false`) | splits | 18.9 s, 239 MB/s | 13.9 s, 324 MB/s | 1.35 |
| an INSERT per task into its staging table, the commit moves them (`write.staging`) | segments | 40.6 s, 111 MB/s | 34.9 s, 129 MB/s | 1.16 |
| **Copy: Spark reads `its_stress` from Cloudberry and writes it into another Cloudberry table** | | | | |
| one transfer each way, one INSERT | segments | 26.1 s, 173 MB/s | 25.7 s, 176 MB/s | 1.02 |
| through staging tables (`write.staging`) | segments | 37.9 s, 119 MB/s | 31.2 s, 145 MB/s | 1.21 |
| **A failure: an executor killed (`kill -9`) 2 s into an export** | | | | |
| a transfer per group of leaf partitions: the parts of its tasks run again | 12 parts | 14.9 s, exact | 11.0 s, exact | 1.35 |
| one transfer of the whole table | segments | fails ² | fails | |

1. The Spark tasks on the Cloudberry side: "segments" — a task per primary segment (min(segments, task slots),
   equal here); "12 parts" — the leaf partitions, as many at once as the task slots; "splits" — a task per Parquet
   split, an INSERT per as many of them as segments, in one transaction.
2. "partition ... of run ... restarted": one transfer cannot be started again by a task.

**What it shows**:

- **A transfer runs a task per segment**, so the segments set its parallelism: 8 segments with 8 task slots make
  an export 1.4 times faster than 4 with 4. Every task and every segment is one single-threaded pipeline (a
  segment scans and formats its rows, a task parses them and writes Parquet): more of them — more speed, while
  the CPU of the machine lasts.
- **A split by leaf partitions** runs its 12 parts on the task slots, every part a transfer of all the segments:
  with a slot per segment the plain transfer is faster — the split wins only while the segments are fewer than
  the task slots. Its point is a failure: a killed executor costs a rerun of its parts, the result is exact —
  one transfer fails.
- **Writes scale with the segments too**: the INSERT runs on every segment at once (a process parsing the
  external table and one inserting after the redistribution by the distribution key) — an import without the
  shuffle 239 -> 324 MB/s. The write tasks wait for the INSERT most of their time: formatting the rows is a small
  part of a write task (a profile — jstack of the tasks every 0.1 s).
- **Not explained, not profiled**: a staging import does not grow with the segments (its commit copies the rows
  once more inside Cloudberry), a copy gains nothing from 4 to 8 segments (173 -> 176 MB/s).
- **Compression** does not matter here: the network of the stand is not the limit.
- **The noise**: one run of every scenario; the same scenario differs by up to ~10% between runs, a few percent
  for an export.
- The full-volume stress test (51.7 GB, `docker/run.sh stress generate` without the row count) was not run: it
  needs ~280 GB of the Docker disk ([docker/data/](docker/data/README.md)).

## Limitations

- A write needs `mode("append")` or `mode("overwrite")`, otherwise "Operation is not supported".
- A classic read uses at most as many tasks as primary segments, and all of them must start within
  `network.timeout` (busy executors — the read fails; lower `parallelism` or use `read.split`).
- A classic read or write does not survive a task restarted midway, speculation is refused — use `read.split` /
  `write.staging`.
- A write is not compressed; read compression on Cloudberry 2.1 — see the defect above.
- `partitionclause` takes the classic syntax with the partitions (`partition by range (d) (start ... every
  ...)`): a PostgreSQL `partition by range (d)` alone makes a table without partitions, a write into it fails.

## Development

- The test stand — HDFS, YARN, Spark 3.5.8, Cloudberry 2.1 in Docker: [docker/](docker/README.md)
  (`docker/run.sh cluster up`).
- The regression on it, 19 runs (local and YARN; reads, writes, streams, split reads, staging writes, types,
  the parser of a read and the writer of a write, compression, failures):
  [src/test/regression](src/test/regression/README.md) (`docker/run.sh regression`).
- The comments marked `tender:` explain the reworked parts of the code and the reasons behind them.

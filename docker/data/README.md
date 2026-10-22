# Test data of the stand

The files of this folder are in the containers of the stand as `/testdata` (read-only):

| Container | For |
|---|---|
| `client` (`docker/run.sh cluster connect`, the regression) | `spark-shell`, `hdfs dfs -put` |
| `cloudberry` | `COPY ... FROM '/testdata/...'` on the Cloudberry side (`gpadmin`) |

```scala
// spark-shell in the client container: a file of the folder, written into Cloudberry
spark.read.option("header", "true").csv("file:///testdata/payments.csv")
  .write.format("cloudberry").options(cb + ("dbtable" -> "payments")).mode("overwrite").save()
```

```sql
-- psql (docker/run.sh cluster psql): straight into a table
copy test.payments from '/testdata/payments.csv' with (format csv, header true);
```

A local-mode `spark-shell` reads `file:///testdata/...` in the client container itself; a YARN application
reads a file put into HDFS first: `hdfs dfs -put /testdata/payments.csv /` in the client container, then
`spark.read.csv("/payments.csv")`.

The folder is on the host: its files outlive `docker/run.sh cluster down`. A file added while the stand is
up is seen at once; the folder itself is mounted at `cluster up`.

## The stress test

The volume check of the connector ([README](../../README.md#performance)): a table of ~50 GB of text (or fewer
rows), read and written whole on YARN (the executors of STAND_RESOURCES, [docker/README.md](../README.md)), an
executor killed during a read.

| File | What |
|---|---|
| [stress.sql](stress.sql) | the table `test.its_stress`: `id bigint, d date, amount numeric(12,2), note text`, 12 monthly leaf partitions, distributed by `id`; generated on the segments at once, in 10 transactions; the text size goes into its comment |
| [stress.scala](stress.scala) | the measurements, three scenarios: **export** — Spark reads `its_stress` from Cloudberry and writes it into HDFS as Parquet (`/stress/its_stress.parquet`), in every read mode (classic, `server.compress`, `read.split=partitions` / `segments`); **import** — Spark reads the Parquet from HDFS and writes it into `its_stress_copy` (with / without the shuffle, `write.staging`); **copy** — Spark reads `its_stress` from Cloudberry and writes it into `its_stress_copy` (classic, `write.staging`). The time is of the transfer, the sink is checked after it (the count and the sums of the Parquet, the rows of the table) |
| [stress-kill.scala](stress-kill.scala) | an export during which an executor is killed (`kill -9`): split by partitions — the parts read and written again, the Parquet checked; classic — fails |

```bash
docker/run.sh stress generate             # 560 million rows: ~60 GB in Cloudberry
docker/run.sh stress generate 50000000    # or fewer: ~4.5 GB of text, 5.5 GB in Cloudberry, 30 s
docker/run.sh stress run                  # ~6 min on 4.5 GB; target/stress/stress.results, *.log
docker/run.sh stress run export,import   # only the steps whose names have that (an import needs an export)
docker/run.sh stress drop
```

**Disk**: the tables live in the Docker disk image (the container of Cloudberry): `its_stress` ~61 GB, the
copy of the writes as much more, the staging write its staging tables too until the commit, and the WAL. A
full `stress run` of 560 million rows grows the image by ~280 GB, and it does not shrink by itself after `stress drop` / `cluster down`: compact the image (fstrim in the Docker VM,
then `Optimize-VHD` / `diskpart compact vdisk` with Docker Desktop stopped). A smaller table — `stress
generate 50000000` (~4.5 GB of text, the same speeds).

`STRESS_KILL_AFTER` — seconds from the start of a read to the kill (default 60; on 4.5 GB a read takes 6–15 s:
`STRESS_KILL_AFTER=5`).

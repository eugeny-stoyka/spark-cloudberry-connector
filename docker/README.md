# Test stand of the connector (Docker)

A self-contained environment for building and checking the connector: the target stack in Linux
containers, the same on any host OS (Windows — Docker Desktop + Git Bash, Linux, macOS). Nothing is
installed on the host except Docker; no other project is needed. Everything goes through
[`docker/run.sh`](run.sh).

What it is for:

- **build** the connector without a local JDK / Maven — `docker/run.sh jar`;
- **regression** — the end-to-end suite of [src/test/regression](../src/test/regression/README.md):
  `spark-shell` runs in local and YARN mode against a real Cloudberry — `docker/run.sh regression`;
- **experiments by hand** — `spark-shell` on YARN next to Cloudberry, the Spark / YARN UIs to see stages,
  tasks, retries of a transfer.

## Quick start

```bash
docker/run.sh jar            # build target/spark-cloudberry-connector_2.12-1.0.jar (~1.5 min the first time)
docker/run.sh cluster up     # build the image if needed, bring the stand up, wait until it is ready (~1 min)
docker/run.sh regression     # the regression suite on the stand (~6 min)
docker/run.sh hosts add      # once: container names in the hosts file, so the UI links work
docker/run.sh cluster down   # remove the stand with all its data
```

`docker/run.sh all` = `jar` → `cluster up` (if not up) → `regression`. The first `cluster up` downloads the
distributions (Hadoop, Spark, the JDBC driver — about 1.1 GB, into `docker/.downloads/`, not in git) and
the base images (`ubuntu:22.04`, `woblerr/cloudberry`).

## What is inside

| Service | What | Version | Web UI |
|---------|------|---------|--------|
| `namenode`, `datanode` | HDFS: the event log of Spark, staging of YARN applications, the Spark jars for YARN | Hadoop 3.3.6 | [namenode:9870](http://namenode:9870), [datanode:9864](http://datanode:9864) |
| `resourcemanager`, `nodemanager` | YARN: executors of the YARN runs (6 GB / 8 vcores for containers by default) | Hadoop 3.3.6 | [resourcemanager:8088](http://resourcemanager:8088), [nodemanager:8042](http://nodemanager:8042) |
| `spark-history` | Spark History Server: finished applications by their event log | Spark 3.5.8 | [spark-history:18080](http://spark-history:18080) |
| `cloudberry` | Apache Cloudberry: the coordinator and `CLOUDBERRY_SEGMENTS` primary segments (4; `large` — 8) in one container (GPORCA on, zstd built in); database `cloudberry`, `gpadmin` / `gpadmin`, schema `test` | Cloudberry 2.1.0-incubating, PostgreSQL 14.4 (`woblerr/cloudberry`) | — (`cloudberry:5432` in the stand network, `localhost:15433` from the host for psql / DBeaver) |
| `client` (on demand) | the Spark client: `spark-shell`, `spark-submit` (YARN client mode or local), `hdfs`, `yarn`; the PostgreSQL JDBC driver in `/opt/jars` | Spark 3.5.8, driver 42.7.4 | — |
| `maven` (on demand) | the build of the connector | Maven 3.9.9, JDK 11 | — |

The Hadoop / Spark services are one image `scc-stand:local` ([Dockerfile](Dockerfile)), the role of a
container is its command ([scripts/entrypoint.sh](scripts/entrypoint.sh)); one JDK 11 for everything.
`client` and `maven` are not started by `cluster up` — `run.sh` runs them for a command and removes them.

## Web UIs

| UI | Address | What to look at |
|----|---------|-----------------|
| YARN ResourceManager | [resourcemanager:8088](http://resourcemanager:8088) | applications; a running Spark application — its Spark UI through the proxy (*ApplicationMaster* link), a finished one — *History* |
| Spark History Server | [spark-history:18080](http://spark-history:18080) | finished Spark applications: jobs, stages, tasks (attempts, failures, durations), SQL plans — e.g. the parts of a `read.split` read and their retries |
| YARN NodeManager | [nodemanager:8042](http://nodemanager:8042) | containers of the node and their logs (stdout / stderr of executors) |
| HDFS NameNode | [namenode:9870](http://namenode:9870) | *Utilities → Browse the file system*: `/spark-history`, `/apps/spark`, `/tmp/logs` |
| HDFS DataNode | [datanode:9864](http://datanode:9864) | where the file links of the NameNode UI lead |

The addresses are by container names — the pages link to each other the same way (YARN — to the
NodeManager and the History Server, HDFS — to the DataNode). On the host the names must point to
127.0.0.1: `docker/run.sh hosts add` once (see [The hosts file](#the-hosts-file)); until then the same pages
open at `localhost:<port>`.

The UI ports on the host are the same as in the containers (for those links). Another stand with the same
ports cannot be up at the same time: bring it down first.

## The hosts file

```bash
docker/run.sh hosts          # the lines for the hosts file and whether they are there
docker/run.sh hosts add      # add them (Windows — an administrator (UAC) prompt, Linux/macOS — sudo)
docker/run.sh hosts update   # rewrite the lines of the stand (delete + add, one prompt)
docker/run.sh hosts delete   # delete them
```

The lines are `127.0.0.1 <name> # scc-stand`, one per name (Windows ignores more than 9 names in a line):
`namenode`, `datanode`, `resourcemanager`, `nodemanager`, `spark-history`. Only lines with this mark are
touched — the lines of other stands stay. Before a change the file is copied to
`<hosts>.scc-stand.bak`; the write is retried while the file is locked (an antivirus holds it after a
change) and rolled back if the result is not complete. To try the commands on a copy without
administrator rights: `STAND_HOSTS_FILE=/tmp/hosts STAND_HOSTS_NO_ELEVATE=1 docker/run.sh hosts add`.

## Commands

| Command | What it does |
|---------|--------------|
| `jar [mvn args]` | builds the connector in the `maven` container from a copy of the sources (`/src` is read-only, the build — in `/build` from scratch) and puts only the jars into `target/`: the shaded `spark-cloudberry-connector_2.12-1.0.jar` and `spark-cloudberry-connector-1.0.0-tests.jar`. IDE builds and `target/regression/` stay as they are. Default goal — `package`; the Maven repository is the volume `scc-stand-maven-repo` |
| `download` | everything the stand takes from the network, only what is missing: the distributions of the image (Hadoop, Spark, the PostgreSQL JDBC driver into `docker/.downloads/`, [scripts/fetch-dists.sh](scripts/fetch-dists.sh)) and the images — Cloudberry (`woblerr/cloudberry`: the stand runs Cloudberry from it, there is no distribution file), `ubuntu:22.04` (the base of the stand image), the Maven one of the build; versions — [.env](.env) |
| `build` | downloads the missing distributions ([scripts/fetch-dists.sh](scripts/fetch-dists.sh): mirrors in turn, resumed, checksums from apache.org / Maven Central) and builds the image; configs and scripts are baked in — a rebuild after editing them takes seconds |
| `cluster up` | `build`, then `docker compose up --wait`: until every service is healthy (Cloudberry — when its init script has created the schema) |
| `cluster status` | the containers and the UI addresses |
| `cluster connect` | bash in a new `client` container |
| `cluster psql` | psql to Cloudberry as `gpadmin`, database `cloudberry` |
| `cluster down` | removes the stand with all its data (also containers left by `connect`) |
| `regression [filter]` | [src/test/regression/run.sh](../src/test/regression/run.sh) on the stand that is up, with the jar of `target/`; `filter` — the runs whose name contains it |
| `all [filter]` | `jar` → `cluster up` (if not up) → `regression` |
| `stress generate [rows] \| run [steps] \| drop` | the stress test of [data/](data/README.md): the table `its_stress` (560 million rows, ~50 GB of text), the measurements and an executor killed during a read, results in `target/stress/`; drop — its tables |
| `hosts [add \| update \| delete]` | the hosts file lines, see above |
| `clean` | `cluster down` and removes the Maven repository volume |
| `help` | the command list |

## Working on the stand

```bash
docker/run.sh cluster connect
```

In the container the connector jars of `target/` are in `/scc`, the sources — in `/src`, the test data of
[data/](data/README.md) — in `/testdata` (read-only; in the `cloudberry` container too, for `COPY`):

```bash
spark-shell --master yarn --num-executors 2 --executor-cores 2 \
  --jars /scc/spark-cloudberry-connector_2.12-1.0.jar,/opt/jars/postgresql-42.7.4.jar
```

```scala
val cb = Map("url" -> "jdbc:postgresql://cloudberry:5432/cloudberry", "user" -> "gpadmin",
  "password" -> "gpadmin", "dbschema" -> "test")
spark.range(1000).selectExpr("id", "id % 7 as k").write.format("cloudberry")
  .options(cb + ("dbtable" -> "my_table")).mode("overwrite").save()
spark.read.format("cloudberry").options(cb + ("dbtable" -> "my_table")).load().groupBy("k").count().show()
```

Local mode works in the same container: `--master local[4]`. Cloudberry from the container —
`cloudberry:5432`, from the host — `localhost:15433` (`psql -h localhost -p 15433 -U gpadmin cloudberry`,
DBeaver).

Spark defaults of the stand ([config/spark/spark-defaults.conf](config/spark/spark-defaults.conf)): YARN
client mode, 2 executors x 2 cores x 1 GB, no dynamic allocation (the connector plans its partitions by the
task slots), in-memory catalog (no Hive metastore), the event log for the History Server. The driver
listens on the IP of its container ([config/spark/spark-env.sh](config/spark/spark-env.sh)): YARN executors
and Cloudberry segments reach it there (the transfer coordinator, gpfdist of local mode tasks).

## Versions and resources

[`.env`](.env) — versions (Hadoop, Spark, the JDBC driver, Cloudberry, the Maven image) and the YARN
resources: `YARN_NM_MEMORY_MB` (6144), `YARN_NM_VCORES` (8), `YARN_CONTAINER_MAX_MB` (4096). docker compose
reads it itself; environment variables win over it:

```bash
YARN_NM_MEMORY_MB=4096 docker/run.sh cluster up   # resources: a new stand, no rebuild of the image
```

Two resource profiles, `STAND_RESOURCES` (variables set explicitly win over a profile):

| Profile | Cloudberry | NodeManager | Containers up to | The stress test |
|---|---|---|---|---|
| `default` | 4 primary segments (`.env`) | 6 GB, 8 vcores (`.env`) | 4 GB | 2 executors x 2 cores x 2 GB |
| `large` | 8 primary segments | 20 GB, 16 vcores | 6 GB | 2 executors x 4 cores x 4 GB |

In both a task slot per segment: a classic transfer runs min(segments, task slots) tasks. Another number of
segments re-initializes Cloudberry (`docker/scripts/cloudberry-start.sh` — the start script of the image with
`CLOUDBERRY_SEGMENTS` of them): its data is gone, `docker/run.sh stress generate` again.

```bash
STAND_RESOURCES=large docker/run.sh cluster up     # the services recreated, Cloudberry initialized with 8 segments
STAND_RESOURCES=large docker/run.sh stress run
docker/run.sh cluster up                           # back to default
```

The first line of `target/stress/stress.results` is the profile and what YARN has (`totalMB`,
`totalVirtualCores`) and the segments of Cloudberry.

A new version of Hadoop / Spark needs a new image: change `.env`, then `docker/run.sh build`.

## Data

**The data lives only in the containers**: HDFS, YARN logs, Cloudberry tables appear with `cluster up`
and disappear with `cluster down` — every stand starts clean. Only the Maven repository (volume
`scc-stand-maven-repo`), the downloaded distributions (`docker/.downloads/`) and the images persist.
The regression results are on the host: `target/regression/`.

**Test data** — files to load — go into [docker/data/](data/README.md): on the host, so they persist, and
`/testdata` in the `client` and `cloudberry` containers.

## Troubleshooting

```bash
docker compose -f docker/docker-compose.yml ps                         # the state of the services
docker compose -f docker/docker-compose.yml logs -f cloudberry          # the log of a service
docker compose -f docker/docker-compose.yml exec resourcemanager yarn logs -applicationId <id>  # executor logs
```

- **`cluster up` waits for long or a service is unhealthy** — the log of that service; Cloudberry needs
  about a minute to initialize.
- **A port is already allocated** — another stand or a local service holds 8088 / 8042
  / 9870 / 9864 / 18080 / 15433: stop it or change the port in [docker-compose.yml](docker-compose.yml).
- **YARN runs wait for containers** — not enough memory for Docker (Docker Desktop: *Settings → Resources*)
  or for YARN: `YARN_NM_MEMORY_MB`.
- **The UI links by name do not open** — `docker/run.sh hosts add`.

## Layout

| Path | What |
|------|------|
| [run.sh](run.sh) | the entry point: build, stand, regression, hosts |
| [docker-compose.yml](docker-compose.yml) | the services of the stand |
| [Dockerfile](Dockerfile) | the image `scc-stand:local`: Hadoop, Spark, the JDBC driver, configs, scripts |
| [.env](.env) | versions and resources |
| [config/hadoop/](config/hadoop/) | `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml` (resources — `@...@` marks filled at the start), `capacity-scheduler.xml` |
| [config/spark/](config/spark/) | `spark-defaults.conf`, `spark-env.sh` |
| [config/cloudberry/init.sql](config/cloudberry/init.sql) | the schema of the regression, run at the Cloudberry init |
| [data/](data/README.md) | test data: `/testdata` in the `client` and `cloudberry` containers (read-only) |
| [scripts/](scripts/) | `entrypoint.sh` (roles), `cluster-init.sh` (HDFS), `fetch-dists.sh`, `build-jar.sh` (the `maven` service), `lib.sh` |

Credentials (`gpadmin` / `gpadmin`) are for the local stand only. The stand has only what the
connector needs (no Hive, Tez, Hue, Kafka).

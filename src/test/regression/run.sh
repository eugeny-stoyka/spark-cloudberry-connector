#!/usr/bin/env bash
# Regression suite of the connector on the docker stand of this repository (docker/: Spark 3.5.8 on
# YARN + Cloudberry 2.1 with CLOUDBERRY_SEGMENTS segments): every run is a spark-shell in the client container of the stand
# fed with a probe from probes/, local and YARN modes, batch and Structured Streaming.
#
# Usage (Git Bash on Windows or Linux): src/test/regression/run.sh [name filter]
# (or docker/run.sh regression [name filter])
#   the stand is up:   docker/run.sh cluster up
#   the jar is built:  target/spark-cloudberry-connector_*.jar (docker/run.sh jar, BUILD.md)
# Variables: CONNECTOR_JAR - the connector jar (default from target/);
#   CB_SCHEMA - schema of the test tables (test, created by the stand).
# Runs one after another: they share the test tables. Results: <run>.results (one line per check,
# DONE at the end) and <run>.log (spark-shell output) in target/regression/.
# A run fails on: exit code, no DONE, FAILED / MISMATCH in the results, errors in the log.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stand="$repo/docker"
CB_SCHEMA="${CB_SCHEMA:-test}"
CONNECTOR_JAR="${CONNECTOR_JAR:-$(ls "$repo"/target/spark-cloudberry-connector_*.jar 2>/dev/null |
  grep -vE -- '-(sources|javadoc)\.jar$' | head -1)}"
LOG_DIR="$repo/target/regression"

[[ -f "${CONNECTOR_JAR:-}" ]] || { echo "connector jar not found: docker/run.sh jar (BUILD.md) or set CONNECTOR_JAR"; exit 1; }
mkdir -p "$LOG_DIR"

# a host path for docker -v: D:/... in Git Bash, as is on Linux
host_path() { (cd "$1" && { pwd -W 2>/dev/null || pwd; }); }
compose() { (cd "$stand" && MSYS_NO_PATHCONV=1 docker compose "$@"); }
cb_sql() { compose exec -T cloudberry su - gpadmin -c "psql -d cloudberry -v ON_ERROR_STOP=1 -q"; }

[[ -n "$(compose ps -q cloudberry 2>/dev/null)" ]] ||
  { echo "Cloudberry of the stand is not running: docker/run.sh cluster up"; exit 1; }

# name | probe | spark-shell options | environment of the probe, or ALLOW_LOG=<regex>: log lines of the
# errors the probe causes on purpose (the rest of the line, '|' included)
RUNS=(
  "names-local1|names|--master local[1]|"
  "parser-local1|parser|--master local[1]|"
  "writer-local1|writer|--master local[1]|"
  "direct-local2|direct|--master local[2]|ALLOW_LOG=division by zero|canceling statement"
  "write-local2|write|--master local[2]|PROBE_PARTITIONS=2"
  "write-local4|write|--master local[4]|PROBE_PARTITIONS=4"
  "nested-local2|nested|--master local[2]|"
  "write-yarn-1x2|write|--master yarn --num-executors 1 --executor-cores 2|PROBE_PARTITIONS=2"
  "write-yarn-2x2|write|--master yarn --num-executors 2 --executor-cores 2|PROBE_PARTITIONS=4"
  "parallelism-yarn-2x2|parallelism|--master yarn --num-executors 2 --executor-cores 2|"
  "parallelism-yarn-1x2|parallelism|--master yarn --num-executors 1 --executor-cores 2|"
  "stats-local2|stats|--master local[2]|"
  "speculation-yarn-2x2|speculation|--master yarn --num-executors 2 --executor-cores 2 --conf spark.speculation=true|"
  "split-local2|split|--master local[2,2]|ALLOW_LOG=injected task failure|Exception in task 0\.0 in stage"
  "types-local2|types|--master local[2]|"
  "staging-local2|staging|--master local[2,2]|ALLOW_LOG=injected task failure|Exception in task 0\.0 in stage|commit for partition 0 |run w/0\.0 aborted|ERROR Utils: Aborting task"
  "backpressure-local4|backpressure|--master local[4]|"
  "limit-local2|limit|--master local[2]|"
  "stream-local2|stream|--master local[2]|"
  "stream-yarn-2x2|stream|--master yarn --num-executors 2 --executor-cores 2|"
)

# source table of the probes: 10 payments 101..110 of the common types
cb_sql <<SQL || { echo "cannot create $CB_SCHEMA.its_payments"; exit 1; }
set client_min_messages = warning;
create schema if not exists $CB_SCHEMA;
drop table if exists $CB_SCHEMA.its_payments;
create table $CB_SCHEMA.its_payments as
select g as payment_id, 1 + g % 4 as client_id, (g * 10.5)::numeric(12,2) as amount,
       date '2026-01-01' + g as payment_date,
       timestamp '2026-01-01 10:00:00' + g * interval '1 hour' as created_at,
       'payment ' || g as note
from generate_series(101, 110) g
distributed by (payment_id);
analyze $CB_SCHEMA.its_payments;
-- a partitioned table for read.split: 100000 rows in 7 monthly leaves (January..July, July empty)
drop table if exists $CB_SCHEMA.its_parts;
create table $CB_SCHEMA.its_parts (id bigint, d date, amount numeric(12,2))
distributed by (id)
partition by range (d) (start (date '2026-01-01') inclusive end (date '2026-08-01') exclusive every (interval '1 month'));
insert into $CB_SCHEMA.its_parts select g, date '2026-01-01' + g % 181, (g % 1000) / 10.0 from generate_series(1, 100000) g;
analyze $CB_SCHEMA.its_parts;
SQL

# the primary segments of the stand (CLOUDBERRY_SEGMENTS): what the probes expect of the transfers
CB_SEGMENTS="$(cb_sql <<< "\pset tuples_only on
select count(*) from gp_segment_configuration where content >= 0 and role = 'p';" | tr -d '[:space:]')"
[[ "$CB_SEGMENTS" =~ ^[0-9]+$ ]] || { echo "cannot query the segments of Cloudberry"; exit 1; }
echo "Cloudberry: $CB_SEGMENTS primary segments"

fails=0
for run in "${RUNS[@]}"; do
  IFS='|' read -r name probe args env <<< "$run"
  [[ -n "${1:-}" && "$name" != *"$1"* ]] && continue
  allow=""
  [[ "$env" != ALLOW_LOG=* ]] || { allow="|${env#ALLOW_LOG=}"; env=""; }
  cb_sql <<< "drop table if exists $CB_SCHEMA.its_payments_copy; drop table if exists $CB_SCHEMA.its_big;" >/dev/null 2>&1
  rm -f "$LOG_DIR/$name.results" "$LOG_DIR/$name.log"
  env_args=(-e "CB_SCHEMA=$CB_SCHEMA" -e "CB_SEGMENTS=$CB_SEGMENTS" -e "RESULTS_FILE=/out/$name.results")
  [[ -z "$env" ]] || env_args+=(-e "$env")
  start=$SECONDS
  code=$(compose run --rm --no-deps -T "${env_args[@]}" -e "ARGS=$args" -e "PROBE=$probe" -e "NAME=$name" \
    -e "JAR=$(basename "$CONNECTOR_JAR")" -e "PG_DRIVER_JAR=${PG_DRIVER_JAR:-}" \
    -v "$(host_path "$(dirname "$CONNECTOR_JAR")"):/scc:ro" \
    -v "$(host_path "$here"):/regression:ro" \
    -v "$(host_path "$LOG_DIR"):/out" \
    client bash -c '
      # PostgreSQL JDBC driver: the connector does not bundle it; the stand image has it in /opt/jars
      pg="${PG_DRIVER_JAR:-$(ls /opt/jars/postgresql-*.jar | head -1)}"
      timeout 600 spark-shell $ARGS --driver-memory 2g --conf spark.dynamicAllocation.enabled=false \
        --conf spark.ui.enabled=false --jars "/scc/$JAR,$pg" < "/regression/probes/$PROBE.scala" > "/out/$NAME.log" 2>&1
      echo $?' 2>/dev/null | tail -1)
  results="$LOG_DIR/$name.results" log="$LOG_DIR/$name.log"
  # not counted: the expected rejections of parallelism=-1 and of spark.speculation=true, the
  # checkpoint write interrupted by Spark itself on stop() of a stream, the ALLOW_LOG lines of the run
  errors=$(grep -E 'NullPointer|Broken pipe|Connection reset|ERROR |Exception|<console>:[0-9]+: error' "$log" 2>/dev/null |
    grep -vcE "parallelism must be an integer|with spark.speculation=true|InterruptedException: sleep interrupted$allow")
  bad=$(grep -cE 'FAILED|MISMATCH' "$results" 2>/dev/null)
  last=$(tail -1 "$results" 2>/dev/null)
  status=OK
  if [[ "$code" != 0 || "$last" != DONE ]] || ((${errors:-0} > 0 || ${bad:-0} > 0)); then
    status=FAIL; fails=$((fails + 1))
  fi
  printf '%-4s %-22s exit=%s errors=%s %3ss\n' "$status" "$name" "${code:-?}" "${errors:-?}" "$((SECONDS - start))"
  grep -v '^DONE$' "$results" 2>/dev/null | sed 's/^/     /' | cut -c1-220
  [[ "$last" == DONE ]] || echo "     (no DONE: the probe stopped half-way, see $log)"
done

cb_sql <<SQL >/dev/null 2>&1
drop table if exists $CB_SCHEMA.its_payments; drop table if exists $CB_SCHEMA.its_payments_copy;
drop table if exists $CB_SCHEMA.its_big; drop table if exists $CB_SCHEMA.its_stream_src;
drop table if exists $CB_SCHEMA.its_stream_sink; drop table if exists $CB_SCHEMA.its_parts;
drop table if exists $CB_SCHEMA.its_types_s; drop table if exists $CB_SCHEMA.its_types_t; drop table if exists $CB_SCHEMA.its_types_c;
drop table if exists $CB_SCHEMA.its_types_g; drop table if exists $CB_SCHEMA.its_types_e;
drop table if exists $CB_SCHEMA.its_types_r; drop table if exists $CB_SCHEMA.its_types_rc; drop table if exists $CB_SCHEMA.its_types_n;
SQL
echo "failed: $fails"
((fails == 0))

#!/usr/bin/env bash
# Test stand of the connector in Docker: HDFS + YARN + Spark 3.5.8 (client, History Server) +
# Cloudberry 2.1, and the build of the connector in a Maven container. Independent of other projects.
#
# Usage (from any directory; on Windows — Git Bash): docker/run.sh <command> [arguments]
#
#   jar [mvn args]       build the connector in a Maven container from a copy of the sources (default:
#                        package); target/ of the working copy gets the jars only:
#                        spark-cloudberry-connector_2.12-1.0.jar and the -tests.jar
#   download             everything the stand takes from the network, only what is missing: the distributions of
#                        its image (Hadoop, Spark, the PostgreSQL JDBC driver -> docker/.downloads/, checksums
#                        checked) and the images (Cloudberry — woblerr/cloudberry, ubuntu:22.04 — the base of the
#                        stand image, the Maven one of the build); versions — docker/.env
#   build                download the distributions (only missing ones) and build the stand image
#   cluster up           bring the stand up and wait until its services are ready
#   cluster status       containers of the stand and the addresses of the web UIs
#   cluster connect      bash on the client node: spark-shell, spark-submit, hdfs dfs, yarn
#   cluster psql         psql to Cloudberry (database cloudberry, user gpadmin)
#   cluster down         remove the stand with all its data
#   regression [filter]  the regression suite (src/test/regression/run.sh) on the stand that is up,
#                        with the jar of target/; filter — runs whose name contains it
#   all                  jar -> cluster up (if not up) -> regression
#   stress generate [rows]  the table of the stress test its_stress in Cloudberry (docker/data/stress.sql),
#                        default 560 million rows: ~50 GB of text, ~60 GB in Cloudberry
#   stress run [steps]   the measurements on YARN, executors of STAND_RESOURCES (default 2 x 2 cores x 2 GB;
#                        docker/data/stress.scala; steps — parts of their
#                        names, comma separated) and an executor killed during a split / a classic read;
#                        results in target/stress/
#   stress drop          drop the tables of the stress test
#   hosts                the hosts file lines for the web UIs (their pages link to each other by
#                        container names) and whether they are there
#   hosts add            add the lines to the system hosts file (Windows — with an administrator
#                        prompt, Linux/macOS — via sudo); lines already there are kept
#   hosts update         rewrite the lines of the stand: delete and add in one go
#   hosts delete         delete the lines of the stand (marked with the comment "# scc-stand")
#   clean                cluster down and remove the Maven repository volume
#   help                 this text
set -euo pipefail

STAND_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$STAND_DIR/.." && pwd)"
# from the stand directory (docker-compose.yml and .env there; a -f path would be passed as /d/... to
# docker on Windows); MSYS_NO_PATHCONV: Git Bash would turn container paths in the arguments (/src,
# /opt/...) into Windows ones
compose() { (cd "$STAND_DIR" && MSYS_NO_PATHCONV=1 docker compose "$@"); }
log() { echo "[$(date +%H:%M:%S)] $*"; }

# Resources of the stand, STAND_RESOURCES:
#   default  Cloudberry of .env (4 primary segments), the YARN NodeManager of .env (6 GB, 8 vcores, containers
#            up to 4 GB); the stress test on 2 executors x 2 cores x 2 GB — a task slot per segment
#   large    Cloudberry with 8 primary segments, the NodeManager 20 GB, 16 vcores, containers up to 6 GB; the
#            stress test on 2 executors x 4 cores x 4 GB — a task slot per segment
# Variables set explicitly win (CLOUDBERRY_SEGMENTS, YARN_NM_MEMORY_MB, YARN_NM_VCORES, YARN_CONTAINER_MAX_MB,
# STRESS_EXECUTORS, STRESS_EXECUTOR_CORES, STRESS_EXECUTOR_MEMORY). The services take them when they start: cluster
# up with the same STAND_RESOURCES — a change recreates the Hadoop / Spark services, and Cloudberry with another
# number of segments is initialized again, without the data of the old one.
case "${STAND_RESOURCES:-default}" in
  default) ;;
  large)
    export CLOUDBERRY_SEGMENTS="${CLOUDBERRY_SEGMENTS:-8}" YARN_NM_MEMORY_MB="${YARN_NM_MEMORY_MB:-20480}" \
      YARN_NM_VCORES="${YARN_NM_VCORES:-16}" YARN_CONTAINER_MAX_MB="${YARN_CONTAINER_MAX_MB:-6144}"
    STRESS_EXECUTORS="${STRESS_EXECUTORS:-2}" STRESS_EXECUTOR_CORES="${STRESS_EXECUTOR_CORES:-4}"
    STRESS_EXECUTOR_MEMORY="${STRESS_EXECUTOR_MEMORY:-4g}"
    ;;
  *) echo "STAND_RESOURCES must be default or large, got: $STAND_RESOURCES" >&2; exit 2 ;;
esac
STRESS_EXECUTORS="${STRESS_EXECUTORS:-2}"
STRESS_EXECUTOR_CORES="${STRESS_EXECUTOR_CORES:-2}"
STRESS_EXECUTOR_MEMORY="${STRESS_EXECUTOR_MEMORY:-2g}"

# Cloudberry is not a distribution file here: the stand runs the image woblerr/cloudberry (the coordinator and the
# segments of a single-node cluster), pulled as the images of the services
download() {
  log "Distributions: Hadoop, Spark, the PostgreSQL JDBC driver -> docker/.downloads/ (only missing ones)"
  bash "$STAND_DIR/scripts/fetch-dists.sh"
  log "Images: Cloudberry (the cloudberry service), the Maven one of the build (the maven service), ubuntu:22.04"
  compose --profile tools pull cloudberry maven
  docker pull -q ubuntu:22.04 >/dev/null
  log "Everything is downloaded: docker/run.sh build (the stand image), docker/run.sh cluster up"
}

build() {
  log "Distributions (only missing ones are downloaded)"
  bash "$STAND_DIR/scripts/fetch-dists.sh"
  log "Building the image scc-stand:local"
  compose build --quiet
}

jar() {
  log "Building the connector (Maven container, the repository is the volume scc-stand-maven-repo)"
  mkdir -p "$ROOT/target"
  compose run --rm maven "$@"
}

# --- the stand -------------------------------------------------------------------------

cluster_exists() { [[ -n "$(compose ps -q namenode 2>/dev/null)" ]]; }

require_cluster() {
  cluster_exists || { log "The stand is not up: docker/run.sh cluster up"; exit 1; }
}

print_ui() {
  log "UI: http://resourcemanager:8088 (YARN: applications, Spark UI of a running one),"
  log "    http://spark-history:18080 (Spark History: jobs, stages, tasks, SQL plans),"
  log "    http://nodemanager:8042 (NodeManager: containers and their logs), http://namenode:9870 (HDFS)"
  log "Cloudberry from the host: localhost:15433, database cloudberry, gpadmin / gpadmin"
  hosts_added || log "For the UI addresses by container names add them to the hosts file: docker/run.sh hosts add"
}

cluster_up() {
  build
  log "Bringing the stand up, waiting for its services"
  compose up -d --wait
  compose ps
  log "The stand is up: docker/run.sh regression; a node — docker/run.sh cluster connect; remove — docker/run.sh cluster down"
  print_ui
}

cluster_down() {
  compose down --remove-orphans
  # one-off containers of compose run (an open connect) are not removed by down
  local ids
  ids="$(docker ps -aq --filter label=com.docker.compose.project=scc-stand)"
  [[ -z "$ids" ]] || docker rm -f $ids >/dev/null
}

cluster() {
  local sub="${1:-status}"
  shift || true
  case "$sub" in
    up)      cluster_up ;;
    down)    cluster_down ;;
    status)
      if cluster_exists; then compose ps; print_ui; else log "The stand is not up (docker/run.sh cluster up)"; fi
      ;;
    # --no-deps: the stand is running, compose run must not recreate its services
    connect) require_cluster; compose run --rm --no-deps client bash ;;
    psql)    require_cluster; compose exec cloudberry su - gpadmin -c "psql -d cloudberry" ;;
    *)
      echo "Unknown command: cluster $sub (up | status | connect | psql | down)" >&2
      exit 2
      ;;
  esac
}

regression() {
  require_cluster
  bash "$ROOT/src/test/regression/run.sh" "$@"
}

# --- the stress test (docker/data/README.md): stress.sql, stress.scala, stress-kill.scala ----------------
STRESS_ROWS_DEFAULT=560000000
host_path() { (cd "$1" && { pwd -W 2>/dev/null || pwd; }); }
cb_psql() { compose exec -T cloudberry su - gpadmin -c "psql -d cloudberry $*"; }

# stress_shell <script of docker/data> <log of target/stress> [docker compose run options]: spark-shell on YARN
stress_shell() {
  local script="$1" log="$2"
  shift 2
  compose run --rm --no-deps -T -e RESULTS_FILE=/out/stress.results -e "STRESS_STEPS=${STRESS_STEPS:-}" "$@" \
    -v "$(host_path "$ROOT/target/stress"):/out" client bash -c \
    "spark-shell --master yarn --num-executors $STRESS_EXECUTORS --executor-cores $STRESS_EXECUTOR_CORES \
       --executor-memory $STRESS_EXECUTOR_MEMORY --driver-memory 2g \
       --conf spark.ui.enabled=false --jars /scc/spark-cloudberry-connector_2.12-1.0.jar,\$(ls /opt/jars/postgresql-*.jar | head -1) \
       < /testdata/$script > /out/$log 2>&1" 2>/dev/null
}

# stress_kill <split | classic>: the read of stress-kill.scala, an executor process killed STRESS_KILL_AFTER
# (60) seconds after it starts
stress_kill() {
  local mode="$1" log="$ROOT/target/stress/kill-$1.log" waited=0
  rm -f "$log"
  stress_shell stress-kill.scala "kill-$mode.log" -e "MODE=$mode" &
  local shell_pid=$!
  until grep -q "^RESULT start" "$log" 2>/dev/null; do
    sleep 2; waited=$((waited + 2))
    ((waited < 600)) || { log "kill $mode: the read did not start"; wait "$shell_pid" || true; return 0; }
  done
  sleep "${STRESS_KILL_AFTER:-60}"
  # the JVM of an executor (comm java), not the bash of its YARN container launch script
  log "kill $mode: kill -9 of an executor: $(compose exec -T nodemanager bash -c \
    'pid=$(ps -eo pid=,comm=,args= | awk "\$2 == \"java\" && /YarnCoarseGrainedExecutorBackend/ {print \$1; exit}");
     [ -n "$pid" ] && kill -9 "$pid" && echo "pid $pid"' 2>/dev/null)"
  wait "$shell_pid" || true
}

stress() {
  local sub="${1:-run}"
  shift || true
  require_cluster
  case "$sub" in
    generate)
      log "its_stress: ${1:-$STRESS_ROWS_DEFAULT} rows (docker/data/stress.sql)"
      cb_psql "-v rows=${1:-$STRESS_ROWS_DEFAULT} -f /testdata/stress.sql"
      ;;
    run)
      [[ -f "$ROOT/target/spark-cloudberry-connector_2.12-1.0.jar" ]] || { echo "No connector jar: docker/run.sh jar" >&2; exit 1; }
      mkdir -p "$ROOT/target/stress"
      # the resources of the run first: what the ResourceManager has (the NodeManager of cluster up), the executors
      local yarn
      yarn="$(compose exec -T resourcemanager curl -s http://localhost:8088/ws/v1/cluster/metrics 2>/dev/null |
        grep -o '"totalMB":[0-9]*\|"totalVirtualCores":[0-9]*' | tr '\n' ' ')"
      local segments
      segments="$(cb_psql "-tAc 'select count(*) from gp_segment_configuration where content >= 0 and role = \$\$p\$\$'" 2>/dev/null | tr -d '[:space:]')"
      echo "resources ${STAND_RESOURCES:-default}: Cloudberry $segments primary segments; YARN $yarn; executors" \
        "${STRESS_EXECUTORS} x ${STRESS_EXECUTOR_CORES} cores x ${STRESS_EXECUTOR_MEMORY}" > "$ROOT/target/stress/stress.results"
      log "$(cat "$ROOT/target/stress/stress.results")"
      log "Measurements (docker/data/stress.scala), the log: target/stress/stress.log"
      STRESS_STEPS="${1:-}" stress_shell stress.scala stress.log
      if [[ -z "${1:-}" ]]; then
        log "An executor killed during a split read, then a classic one"
        stress_kill split
        stress_kill classic
      fi
      cat "$ROOT/target/stress/stress.results"
      ;;
    drop) cb_psql "-c 'drop table if exists test.its_stress; drop table if exists test.its_stress_copy'" ;;
    *)
      echo "Unknown command: stress $sub (generate | run | drop)" >&2
      exit 2
      ;;
  esac
}

# --- the hosts file for the web UIs ------------------------------------------------------
# The YARN / HDFS / Spark History pages link to each other by container names: on the host the names must
# point to 127.0.0.1 (the UI ports are published as they are in the containers, docker-compose.yml)
HOSTS_NAMES="namenode datanode resourcemanager nodemanager spark-history"
HOSTS_MARK="# scc-stand"
# a line per name: Windows takes no more than 9 names in a line of the hosts file and silently skips the
# rest (the name is not resolved)
hosts_lines() {
  local name
  for name in $HOSTS_NAMES; do echo "127.0.0.1 $name $HOSTS_MARK"; done
}
# STAND_HOSTS_FILE / STAND_HOSTS_NO_ELEVATE=1 — trying the commands on a copy of the file without
# administrator rights
is_windows() { [[ -d /c/Windows/System32 ]]; }
hosts_file() {
  if [[ -n "${STAND_HOSTS_FILE:-}" ]]; then echo "$STAND_HOSTS_FILE"
  elif is_windows; then echo /c/Windows/System32/drivers/etc/hosts
  else echo /etc/hosts
  fi
}

# hosts_added: the hosts file has the lines of the stand for all the names (a check of the file, not of
# DNS: on Windows a failed name lookup takes seconds via LLMNR/NetBIOS)
hosts_added() {
  local file line
  file="$(hosts_file)"
  [[ -r "$file" ]] || return 1
  while IFS= read -r line; do
    grep -Fqx -e "$line" -e "$line"$'\r' "$file" || return 1
  done < <(hosts_lines)
}

# windows_elevated <PowerShell code>: run the code as administrator (the UAC prompt)
windows_elevated() {
  local script
  script="$(mktemp --suffix=.ps1)"
  printf '%s\r\n' "$1" > "$script"
  # a refusal in the UAC prompt is not an error of the script: the result is checked by the hosts file
  if [[ "${STAND_HOSTS_NO_ELEVATE:-}" == 1 ]]; then
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$(cygpath -w "$script")" || true
  else
    powershell.exe -NoProfile -Command \
      "Start-Process powershell -Verb RunAs -Wait -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','$(cygpath -w "$script")'" ||
      true
  fi
  rm -f "$script"
}

# hosts_update <add | delete>: remove the lines of the stand (marked with HOSTS_MARK) and, for add, append
# the current ones. The file must not end up empty or cut: before the change — a copy
# <hosts>.scc-stand.bak (if the file is not empty), one write with a retry on a lock (an antivirus holds
# the hosts file after a change), a check of the line count and a rollback to the original content
hosts_update() {
  local mode="$1" ps_lines
  # the lines of the stand as a PowerShell array: '127.0.0.1 a # scc-stand','127.0.0.1 b # scc-stand'
  ps_lines="$(hosts_lines | sed "s/.*/'&'/" | paste -sd, -)"
  if is_windows; then
    windows_elevated "\$ErrorActionPreference = 'Stop'
\$p = '$(cygpath -w "$(hosts_file)")'
# the read is retried too; not read — nothing is written (the file would be replaced with a stub)
\$orig = \$null
for (\$i = 1; \$i -le 20 -and \$null -eq \$orig; \$i++) {
  try { \$orig = [IO.File]::ReadAllLines(\$p) } catch { Start-Sleep -Milliseconds 500 }
}
if (\$null -eq \$orig) { exit 1 }
if (\$orig.Length -gt 0) { [IO.File]::WriteAllLines(\$p + '.scc-stand.bak', \$orig) }
\$new = [Collections.Generic.List[string]]::new()
foreach (\$l in \$orig) { if (\$l -notmatch '$HOSTS_MARK\s*\$') { \$new.Add(\$l) } }
if ('$mode' -eq 'add') { foreach (\$h in @($ps_lines)) { \$new.Add(\$h) } }
\$ok = \$false
for (\$i = 1; \$i -le 10 -and -not \$ok; \$i++) {
  try { [IO.File]::WriteAllLines(\$p, \$new.ToArray()); \$ok = ([IO.File]::ReadAllLines(\$p)).Length -eq \$new.Count }
  catch { Start-Sleep -Milliseconds 500 }
}
if (-not \$ok -and \$orig.Length -gt 0) {
  for (\$i = 1; \$i -le 10; \$i++) { try { [IO.File]::WriteAllLines(\$p, \$orig); break } catch { Start-Sleep -Milliseconds 500 } }
}"
  else
    local file append=""
    file="$(hosts_file)"
    [[ "$mode" == add ]] && append="printf '%s\n' $(hosts_lines | sed "s/.*/'&'/" | paste -sd' ' -);"
    # cat > instead of mv: the mode and the owner of /etc/hosts are kept
    sudo sh -c "[ -s '$file' ] && cp '$file' '$file.scc-stand.bak'; \
      { grep -v '$HOSTS_MARK\$' '$file'; $append } > '$file.scc-stand' && \
      cat '$file.scc-stand' > '$file' && rm '$file.scc-stand'"
  fi
}

# require_hosts_readable: the file is not held by another process (an antivirus holds it after a change)
require_hosts_readable() {
  cat "$(hosts_file)" >/dev/null 2>&1 ||
    { log "$(hosts_file) is held by another process now — repeat the command in a few seconds"; exit 1; }
}

# hosts_add [force]: append the lines of the stand; force (hosts update) — rewrite them even if they are
# there already (delete and add in one script, one UAC prompt)
hosts_add() {
  local file backup=""
  file="$(hosts_file)"
  require_hosts_readable
  if [[ "${1:-}" != force ]] && hosts_added; then
    log "The lines of the stand are in $file already (rewrite them — docker/run.sh hosts update)"
    return 0
  fi
  # hosts_update makes a copy of a non-empty file only
  if [[ -s "$file" ]]; then
    backup=" (a copy of the previous file — $file.scc-stand.bak)"
  else
    log "WARNING: $file is empty — it will have the lines of the stand only"
  fi
  # earlier lines of the stand (e.g. without the name of a new service) are replaced with the current ones
  log "Writing to $file: 127.0.0.1 <name> $HOSTS_MARK for $HOSTS_NAMES"
  hosts_update add
  if hosts_added; then
    log "Done: the web UIs open by container names$backup"
  else
    log "The lines are not added (refused in the UAC prompt or the file is locked); the file is not changed"
    exit 1
  fi
}

hosts_delete() {
  local file
  file="$(hosts_file)"
  require_hosts_readable
  if ! grep -q "$HOSTS_MARK.\?\$" "$file"; then
    log "$file has no lines of the stand"
    return 0
  fi
  log "Deleting the lines of the stand from $file"
  hosts_update delete
  if grep -q "$HOSTS_MARK.\?\$" "$file"; then
    log "The lines are not deleted (refused in the UAC prompt or the file is locked); the file is not changed"
    exit 1
  fi
  log "Done (a copy of the previous file — $file.scc-stand.bak)"
}

hosts() {
  case "${1:-}" in
    add)    hosts_add ;;
    update) hosts_add force ;;
    delete) hosts_delete ;;
    "")
      echo "The hosts file lines ($(hosts_file)) for the web UIs:"
      hosts_lines | sed 's/^/  /'
      if hosts_added; then
        echo "The lines are there — the web UIs open by container names (docker/run.sh cluster status)."
      elif grep -q "$HOSTS_MARK.\?\$" "$(hosts_file)" 2>/dev/null; then
        echo "The lines of the stand are outdated — update: docker/run.sh hosts update (administrator rights)."
      else
        echo "No lines: add them — docker/run.sh hosts add (administrator rights)."
      fi
      ;;
    *)
      echo "Unknown command: hosts $1 (add | update | delete)" >&2
      exit 2
      ;;
  esac
}

# main: the whole command dispatch in one function — bash reads it completely before running it, so
# editing the script during a long run does not break its tail
main() {
  local command="${1:-help}"
  shift || true
  case "$command" in
    jar)        jar "$@" ;;
    download)   download ;;
    build)      build ;;
    cluster)    cluster "$@" ;;
    regression) regression "$@" ;;
    all)
      jar
      cluster_exists || cluster_up
      regression "$@"
      ;;
    hosts)      hosts "$@" ;;
    stress)     stress "$@" ;;
    clean)
      cluster_down
      docker volume rm scc-stand-maven-repo >/dev/null 2>&1 || true
      ;;
    -h|--help|help)
      sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'
      ;;
    *)
      echo "Unknown command: $command (see $0 help)" >&2
      exit 2
      ;;
  esac
}

main "$@"; exit $?

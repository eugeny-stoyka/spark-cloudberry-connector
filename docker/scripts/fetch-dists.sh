#!/usr/bin/env bash
# Downloads the distributions of the stand image into docker/.downloads/.
#
# Files are downloaded in parallel and resumed (curl -C -), the mirrors are tried in turn until a file
# is complete; the integrity is checked against the checksums of apache.org / Maven Central (taken
# from there, not from a mirror). A second run skips the files checked already, so an interrupted
# download is resumed by running the script again.
#
# Usage: docker/scripts/fetch-dists.sh  (versions — docker/.env or environment variables)
# Variables:
#   APACHE_MIRRORS — Apache dist mirrors separated by spaces (the order is the priority)
#   MAVEN_REPO     — Maven repository of the JDBC driver
#   MAX_ROUNDS     — how many times to go over the mirrors before giving up
set -uo pipefail

cd "$(dirname "$0")/.."
# versions from .env; environment variables win, as for docker compose
eval "$(sed -nE 's/^([A-Z_]+)=(.*)$/\1="${\1:-\2}"/p' .env)"

DOWNLOADS="${DOWNLOADS:-.downloads}"
APACHE_MIRRORS="${APACHE_MIRRORS:-https://downloads.apache.org https://archive.apache.org/dist https://mirrors.aliyun.com/apache https://repo.huaweicloud.com/apache}"
APACHE_CHECKSUMS="${APACHE_CHECKSUMS:-https://archive.apache.org/dist}"
MAVEN_REPO="${MAVEN_REPO:-https://repo1.maven.org/maven2}"
MAX_ROUNDS="${MAX_ROUNDS:-30}"

mkdir -p "$DOWNLOADS"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# fetch <file> <checksum url> <url1> [url2 ...]
fetch() {
  local file="$1" sum_url="$2"; shift 2
  local target="$DOWNLOADS/$file" ok_mark="$DOWNLOADS/$file.ok"

  if [[ -f "$ok_mark" && -f "$target" ]]; then
    log "$file: downloaded and checked already"
    return 0
  fi

  local round url
  for ((round = 1; round <= MAX_ROUNDS; round++)); do
    for url in "$@"; do
      log "$file: attempt $round, $url"
      # --speed-limit/--speed-time: a stalled connection is dropped, the next attempt resumes
      if curl -fL --retry 3 --retry-delay 5 -C - --speed-limit 2048 --speed-time 60 \
           -sS -o "$target" "$url"; then
        if verify "$file" "$sum_url"; then
          touch "$ok_mark"
          log "$file: OK ($(du -h "$target" | cut -f1))"
          return 0
        fi
        log "$file: checksum mismatch, downloading again"
        rm -f "$target"
      fi
    done
  done
  log "$file: not downloaded in $MAX_ROUNDS rounds"
  return 1
}

# verify <file> <checksum url>: the algorithm by the length of the hex string (sha1/sha256/sha512)
verify() {
  local file="$1" sum_url="$2" expected actual
  expected=$(curl -fsSL --retry 5 --max-time 120 "$sum_url" | tr -d ' \n\r' | grep -oiE '[0-9a-f]{128}|[0-9a-f]{64}|[0-9a-f]{40}' | head -1 | tr 'A-F' 'a-f')
  if [[ -z "$expected" ]]; then
    log "$file: no checksum ($sum_url)"
    return 1
  fi
  case ${#expected} in
    128) actual=$(sha512sum "$DOWNLOADS/$file" | cut -d' ' -f1) ;;
    64)  actual=$(sha256sum "$DOWNLOADS/$file" | cut -d' ' -f1) ;;
    40)  actual=$(sha1sum   "$DOWNLOADS/$file" | cut -d' ' -f1) ;;
  esac
  [[ "$actual" == "$expected" ]]
}

# apache <path in dist> <file> <checksum extension>
apache() {
  local path="$1" file="$2" sum_ext="$3" urls=() m
  for m in $APACHE_MIRRORS; do urls+=("$m/$path/$file"); done
  fetch "$file" "$APACHE_CHECKSUMS/$path/$file.$sum_ext" "${urls[@]}"
}

PG_JAR="postgresql-${POSTGRES_JDBC_VERSION}.jar"
PG_URL="$MAVEN_REPO/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/$PG_JAR"

pids=()
apache "hadoop/common/hadoop-${HADOOP_VERSION}" "hadoop-${HADOOP_VERSION}.tar.gz" sha512 & pids+=($!)
apache "spark/spark-${SPARK_VERSION}" "spark-${SPARK_VERSION}-bin-hadoop3.tgz" sha512 & pids+=($!)
fetch "$PG_JAR" "$PG_URL.sha1" "$PG_URL" & pids+=($!)

failed=0
for pid in "${pids[@]}"; do wait "$pid" || failed=1; done

if ((failed)); then
  log "SOME FILES ARE NOT DOWNLOADED — run the script again, the download resumes"
  exit 1
fi
log "ALL DISTRIBUTIONS ARE DOWNLOADED: $DOWNLOADS"

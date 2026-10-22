#!/usr/bin/env bash
# Service maven of the stand: builds the connector from a copy of the sources (/src is mounted
# read-only, the build goes in /build inside the container, from scratch every time) — target/ of the
# working copy (IDE builds, regression results) is not touched, only the jars are put into /out
# (target/ on the host). Arguments go to mvn (default: package).
set -euo pipefail

log() { echo "[$(date +%H:%M:%S)] $*"; }

log "Copying the sources /src -> /build"
rm -rf /build && mkdir -p /build
(cd /src && tar cf - --exclude=./.git --exclude=./.idea --exclude=./target --exclude=./docker/.downloads .) |
  (cd /build && tar xf -)

if (($# == 0)); then set -- package; fi
cd /build
mvn -B -ntp "$@"

mkdir -p /out
cp target/spark-cloudberry-connector_*.jar target/spark-cloudberry-connector-*-tests.jar /out/
log "Jars in target/: $(cd target && ls spark-cloudberry-connector_*.jar spark-cloudberry-connector-*-tests.jar | paste -sd' ')"

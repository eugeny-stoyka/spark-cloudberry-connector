#!/usr/bin/env bash
# One-off preparation of HDFS after the start of the stand: service directories and the Spark jars
# for YARN. Idempotent: a second run overwrites nothing.
set -euo pipefail

. /opt/stand/scripts/lib.sh

wait_port namenode 8020
log "Waiting for the NameNode to leave safe mode"
hdfs dfsadmin -safemode wait

log "Waiting for the DataNode to register"
until [[ "$(hdfs dfsadmin -report -live 2>/dev/null | grep -c '^Name:')" -ge 1 ]]; do sleep 2; done

hdfs dfs -mkdir -p /tmp /tmp/logs /user/root /apps/spark /spark-history
hdfs dfs -chmod 1777 /tmp /tmp/logs /spark-history

if ! hdfs dfs -test -e /apps/spark/spark-jars.zip; then
  log "Putting the Spark jars into HDFS (spark.yarn.archive)"
  # not compressed: YARN only unpacks the archive, the jars are compressed already
  (cd "$SPARK_HOME/jars" && "$JAVA_HOME/bin/jar" cf0M /tmp/spark-jars.zip *.jar)
  hdfs dfs -put /tmp/spark-jars.zip /apps/spark/
  rm -f /tmp/spark-jars.zip
fi

log "HDFS is ready"

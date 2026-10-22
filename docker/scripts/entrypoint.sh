#!/usr/bin/env bash
# Entry point of the stand image: the first argument is the role of the container.
# An unknown role runs as a plain command (bash, spark-shell, spark-submit, hdfs, yarn, ...).
set -euo pipefail

. /opt/stand/scripts/lib.sh

role="${1:-bash}"
shift || true

render_config

case "$role" in
  namenode)
    if [[ ! -d /data/dfs/name/current ]]; then
      log "Formatting the NameNode"
      hdfs namenode -format -force -nonInteractive -clusterId scc-stand
    fi
    exec hdfs namenode
    ;;
  datanode)
    exec hdfs datanode
    ;;
  resourcemanager)
    exec yarn resourcemanager
    ;;
  nodemanager)
    exec yarn nodemanager
    ;;
  cluster-init)
    exec /opt/stand/scripts/cluster-init.sh
    ;;
  spark-history)
    # in the foreground (start-history-server.sh would go to the background)
    wait_port namenode 8020
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
    ;;
  *)
    exec "$role" "$@"
    ;;
esac

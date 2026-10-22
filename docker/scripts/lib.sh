# Common functions of the stand scripts (sourced with ".").

log() { echo "[$(date +%H:%M:%S)] $*"; }

# wait_port <host> <port> [timeout, s]
wait_port() {
  local host="$1" port="$2" timeout="${3:-300}" waited=0
  until nc -z "$host" "$port" 2>/dev/null; do
    if ((waited >= timeout)); then
      log "$host:$port is not up in ${timeout}s"
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

# render_config: the resources of docker/.env (environment of the container) in place of the @NAME@ marks
# of yarn-site.xml; from an untouched copy <file>.stand.orig on every start, so new values need a new
# container, not a new image
render_config() {
  local file="$HADOOP_CONF_DIR/yarn-site.xml"
  [[ -f "$file.stand.orig" ]] || cp "$file" "$file.stand.orig"
  sed -e "s|@YARN_NM_MEMORY_MB@|${YARN_NM_MEMORY_MB:-6144}|g" \
      -e "s|@YARN_NM_VCORES@|${YARN_NM_VCORES:-8}|g" \
      -e "s|@YARN_CONTAINER_MAX_MB@|${YARN_CONTAINER_MAX_MB:-4096}|g" \
      "$file.stand.orig" > "$file"
}

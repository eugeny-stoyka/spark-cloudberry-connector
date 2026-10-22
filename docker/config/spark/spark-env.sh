# test stand: environment of the Spark client (spark-submit, spark-shell in a container).
# The driver in client mode: YARN executors and Cloudberry segments reach it (the transfer coordinator,
# gpfdist of local mode tasks) by the IP of the container, not by its name
export SPARK_LOCAL_HOSTNAME="${SPARK_LOCAL_HOSTNAME:-$(hostname -i | awk '{print $1}')}"

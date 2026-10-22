# Image of the test stand of the connector: HDFS, YARN, Spark History Server and the Spark client
# (spark-shell / spark-submit, YARN client mode). The role of a container is its command
# (scripts/entrypoint.sh). Distributions come from .downloads/ (scripts/fetch-dists.sh), the network
# is needed for apt only. Cloudberry is a separate image (woblerr/cloudberry, docker-compose.yml).
#
# One JDK 11 for everything: Hadoop 3.3 runs on Java 11, the connector is built for it (-release:11).
FROM ubuntu:22.04

ARG HADOOP_VERSION
ARG SPARK_VERSION
ARG POSTGRES_JDBC_VERSION

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=UTC

# man1 is missing in the minimal ubuntu image (the postinst of openjdk-11 fails without it)
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
         openjdk-11-jdk-headless bash ca-certificates curl netcat-openbsd procps \
    && rm -rf /var/lib/apt/lists/*

# the PostgreSQL JDBC driver is not in the connector jar (provided): /opt/jars for --jars
RUN --mount=type=bind,source=.downloads,target=/dists \
    set -eux; \
    unpack() { mkdir -p "$2"; tar -xzf "$1" -C "$2" --strip-components=1; }; \
    unpack /dists/hadoop-${HADOOP_VERSION}.tar.gz /opt/hadoop; \
    rm -rf /opt/hadoop/share/doc; \
    unpack /dists/spark-${SPARK_VERSION}-bin-hadoop3.tgz /opt/spark; \
    mkdir -p /opt/jars; \
    cp /dists/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/jars/

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_HOME=/opt/spark \
    USER=root
ENV PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${PATH}

COPY config/hadoop/ ${HADOOP_CONF_DIR}/
COPY config/spark/ ${SPARK_HOME}/conf/
COPY scripts/ /opt/stand/scripts/
RUN chmod +x /opt/stand/scripts/*.sh

ENTRYPOINT ["/opt/stand/scripts/entrypoint.sh"]
CMD ["bash"]

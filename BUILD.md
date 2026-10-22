# Building Tender Spark Cloudberry Connector

The library is built with Maven. It targets Spark 3.5.8 / Scala 2.12 and is compiled against the JDK 11
API (`-release:11`), thus you need JDK 11 for the build process, e.g. install openjdk version 11 on your
machine and define JAVA_HOME environment variable pointing to JVM directory:
`export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

Then go to the source tree root to start a build.

## Source tree
```
pom.xml                  the only build file
src/main/scala           the connector
src/main/resources       META-INF/services (the data source name), log4j2.xml,
                         com/tender/cloudberry/connector/project.properties (filtered)
src/test/scala           the test application (com.tender.cloudberry.connector.testapp, examples/run-test-app.sh)
src/test/resources       log4j.properties of the test application
src/test/regression      the regression suite on the docker stand (README.md there)
docker                   the test stand: HDFS, YARN, Spark, Cloudberry; the build in Docker (README.md there)
examples                 spark-shell examples
```

## Maven
Apache Maven 3.x.x is required (3.9.x is used now). Install it and run:
```bash
mvn clean package
```
Or in Docker without local Maven/JDK (a Maven container builds a copy of the sources, only the jars are
put into target/; the Maven repository is the volume `scc-stand-maven-repo`):
```bash
docker/run.sh jar
```
Look for resulting JAR in the target/ folder: `target/spark-cloudberry-connector_2.12-1.0.jar` (the shaded one,
put it into Spark), `target/spark-cloudberry-connector-1.0.0-tests.jar` — the test application.
Also there is `install` target which will copy the resulting file into /home/hadoop/spark/jars/ directory.

The shaded jar has its own copies of `commons-dbcp2` / `commons-pool2` in `com.tender.cloudberry.connector.shaded.*`:
no clash with the Kafka connector on one classpath.

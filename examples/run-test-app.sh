## To run under debugger add:
#       --conf spark.driver.extraJavaOptions="-Djava.net.preferIPv4Stack=True -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=localhost:5005" \
#       --conf spark.executor.heartbeatInterval=1h \
#       --conf spark.network.timeout=12h \

## Example for run spark-cloudberry-connector test application
## Pass the number of rows to generate/insert as argument for this script
## Run from the root of the repository after the build (BUILD.md): the application is in
## target/spark-cloudberry-connector-<version>-tests.jar (src/test/scala), its dependencies come as
## --packages, the connector itself - from the Spark jars folder (mvn install)
log4j_file="src/test/resources/log4j.properties"
log4j_setting="-Dlog4j.configuration=file:log4j.properties"

spark-submit --master yarn --deploy-mode client \
        --num-executors 2 \
        --executor-cores 1 \
        --executor-memory 1G \
        --driver-memory 1G \
        --files "${log4j_file}" \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${log4j_file}" \
        --conf "spark.executor.extraJavaOptions=${log4j_setting}" \
        --packages com.vividsolutions:jts:1.13,com.typesafe.scala-logging:scala-logging_2.12:3.8.0 \
        --conf spark.default.parallelism=10 \
        --conf spark.task.cpus=1 \
        --conf spark.cloudberry.jdbc.url=<jdbc:postgresql://host:port/database> \
        --conf spark.cloudberry.user=<dbuser> \
        --conf spark.cloudberry.password=<dbpassword> \
        --conf spark.cloudberry.dbtable=<tablename> \
        --conf spark.cloudberry.mode=<overwrite|append> \
        --conf spark.cloudberry.buffer.size=20000 \
        --class com.tender.cloudberry.connector.testapp.ItsCbConnectorTestApp \
        target/spark-cloudberry-connector-1.0.0-tests.jar \
        $@

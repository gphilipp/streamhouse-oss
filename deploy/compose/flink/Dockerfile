# Flink 2.1 with the connectors the streamhouse needs:
#  - Kafka (upsert-kafka / kafka) SQL connector
#  - Avro with a Confluent-compatible schema registry (we point it at Apicurio's ccompat API)
#  - Iceberg (REST catalog served by Gravitino) + AWS bundle for S3FileIO against SeaweedFS
#  - Hadoop client (Iceberg's Flink catalog loader needs org.apache.hadoop.conf.Configuration)
#  - S3 filesystem plugin for checkpoints
#  - the streamhouse topic catalog (flink-catalog module): every Kafka topic is a table
# Build context: this directory, plus the named context `catalog` = flink-catalog/target
# (see docker-compose.yml). Run `make build` first.
FROM apache/flink:2.1.3-java21

ARG MAVEN=https://repo1.maven.org/maven2
ARG KAFKA_CONNECTOR=5.0.0-2.1
ARG FLINK=2.1.3
ARG ICEBERG=1.12.0
ARG HADOOP=3.4.1

RUN set -eux; \
    cd /opt/flink/lib; \
    for url in \
      "$MAVEN/org/apache/flink/flink-sql-connector-kafka/$KAFKA_CONNECTOR/flink-sql-connector-kafka-$KAFKA_CONNECTOR.jar" \
      "$MAVEN/org/apache/flink/flink-sql-avro-confluent-registry/$FLINK/flink-sql-avro-confluent-registry-$FLINK.jar" \
      "$MAVEN/org/apache/iceberg/iceberg-flink-runtime-2.1/$ICEBERG/iceberg-flink-runtime-2.1-$ICEBERG.jar" \
      "$MAVEN/org/apache/iceberg/iceberg-aws-bundle/$ICEBERG/iceberg-aws-bundle-$ICEBERG.jar" \
      "$MAVEN/org/apache/hadoop/hadoop-client-api/$HADOOP/hadoop-client-api-$HADOOP.jar" \
      "$MAVEN/org/apache/hadoop/hadoop-client-runtime/$HADOOP/hadoop-client-runtime-$HADOOP.jar" \
    ; do curl -fsSL -O "$url"; done; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-$FLINK.jar /opt/flink/plugins/s3-fs-hadoop/; \
    chown -R flink:flink /opt/flink/lib /opt/flink/plugins

# Catalogs registered in every SQL Gateway session through the file catalog store:
# `streamhouse` (topics as tables) and `lake` (Iceberg).
COPY --from=catalog --chown=flink:flink streamhouse-flink-catalog-*.jar /opt/flink/lib/
COPY --chown=flink:flink catalogs/ /opt/flink/catalogs/

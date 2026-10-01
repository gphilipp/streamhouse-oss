package org.streamhouseoss.controlplane.api;

import java.util.Map;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/** Postgres for the desired state and Kafka for topic checks. */
public class StateResource implements QuarkusTestResourceLifecycleManager {

    private KafkaContainer kafka;
    private PostgreSQLContainer postgres;

    @Override
    public Map<String, String> start() {
        kafka = new KafkaContainer("apache/kafka:4.3.1");
        postgres = new PostgreSQLContainer("postgres:17").withDatabaseName("streamhouse");
        kafka.start();
        postgres.start();
        return Map.of(
                "kafka.bootstrap.servers", kafka.getBootstrapServers(),
                "quarkus.datasource.jdbc.url", postgres.getJdbcUrl(),
                "quarkus.datasource.username", postgres.getUsername(),
                "quarkus.datasource.password", postgres.getPassword());
    }

    @Override
    public void stop() {
        if (postgres != null) {
            postgres.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }
}

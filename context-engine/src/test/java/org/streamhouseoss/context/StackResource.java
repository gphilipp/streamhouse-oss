package org.streamhouseoss.context;

import java.time.Duration;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/** Kafka, Postgres and Apicurio Registry, the context engine's runtime dependencies. */
public class StackResource implements QuarkusTestResourceLifecycleManager {

    static KafkaContainer kafka;
    static PostgreSQLContainer postgres;
    static GenericContainer<?> registry;

    @Override
    public Map<String, String> start() {
        kafka = new KafkaContainer("apache/kafka:4.3.1");
        postgres = new PostgreSQLContainer("postgres:17").withDatabaseName("streamhouse");
        registry = new GenericContainer<>("apicurio/apicurio-registry:3.3.3")
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200))
                .withStartupTimeout(Duration.ofMinutes(2));
        kafka.start();
        postgres.start();
        registry.start();
        return Map.of(
                "kafka.bootstrap.servers", kafka.getBootstrapServers(),
                "quarkus.datasource.jdbc.url", postgres.getJdbcUrl() + "&reWriteBatchedInserts=true",
                "quarkus.datasource.username", postgres.getUsername(),
                "quarkus.datasource.password", postgres.getPassword(),
                "streamhouse.context.registry-url", registryUrl());
    }

    static String registryUrl() {
        return "http://" + registry.getHost() + ":" + registry.getMappedPort(8080) + "/apis/ccompat/v7";
    }

    @Override
    public void stop() {
        if (registry != null) {
            registry.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }
}

package org.streamhouseoss.controlplane.reconcile;

import java.util.LinkedHashMap;
import java.util.Map;

import org.jboss.logging.Logger;
import org.streamhouseoss.controlplane.StreamhouseConfig;
import org.streamhouseoss.controlplane.clients.Gravitino;
import org.streamhouseoss.controlplane.flink.FlinkDdl;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Registers the platform in Gravitino: the metalake, a messaging catalog over Kafka (topics are
 * discovered live) and the Iceberg catalog that topics are materialized into (same JDBC backend as the REST
 * service, so tables appear as soon as Flink creates them).
 */
@ApplicationScoped
public class CatalogBootstrap {

    private static final Logger LOG = Logger.getLogger(CatalogBootstrap.class);

    private final Gravitino gravitino;
    private final StreamhouseConfig config;
    private volatile boolean done;

    public CatalogBootstrap(Gravitino gravitino, StreamhouseConfig config) {
        this.gravitino = gravitino;
        this.config = config;
    }

    /** Retries until Gravitino is reachable, then stops. */
    @Scheduled(every = "15s", delayed = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void bootstrap() {
        if (done) {
            return;
        }
        try {
            gravitino.bootstrap(kafkaCatalog(), icebergCatalog());
            done = true;
        } catch (RuntimeException e) {
            LOG.warnf("Gravitino bootstrap pending: %s", e.getMessage());
        }
    }

    Map<String, Object> kafkaCatalog() {
        return Map.of("name", "kafka", "type", "MESSAGING", "provider", "kafka", "comment", "Kafka topics",
                "properties", Map.of("bootstrap.servers", config.internal().kafkaBootstrap()));
    }

    Map<String, Object> icebergCatalog() {
        StreamhouseConfig.Lake lake = config.lake();
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("catalog-backend", "jdbc");
        properties.put("catalog-backend-name", FlinkDdl.LAKE);
        properties.put("uri", lake.jdbcUrl());
        properties.put("jdbc-driver", "org.postgresql.Driver");
        properties.put("jdbc-user", lake.jdbcUser());
        properties.put("jdbc-password", lake.jdbcPassword());
        properties.put("jdbc-initialize", "true");
        properties.put("warehouse", lake.warehouse());
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3-endpoint", lake.s3Endpoint());
        properties.put("s3-region", lake.s3Region());
        properties.put("s3-access-key-id", lake.s3AccessKey());
        properties.put("s3-secret-access-key", lake.s3SecretKey());
        properties.put("s3-path-style-access", "true");
        return Map.of("name", FlinkDdl.LAKE, "type", "RELATIONAL", "provider", "lakehouse-iceberg",
                "comment", "Iceberg tables materialized from topics", "properties", properties);
    }
}

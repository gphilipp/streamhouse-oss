package org.streamhouseoss.controlplane.reconcile;

import java.util.LinkedHashMap;
import java.util.Map;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.streamhouseoss.controlplane.StreamhouseConfig;
import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.Gravitino;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Registers the platform in Gravitino: the metalake, a messaging catalog over Kafka (topics are
 * discovered live) and the Iceberg catalog Tableflow writes to (same JDBC backend as the REST
 * service, so tables appear as soon as Flink creates them).
 */
@ApplicationScoped
public class CatalogBootstrap {

    private static final Logger LOG = Logger.getLogger(CatalogBootstrap.class);

    private final Gravitino gravitino;
    private final StreamhouseConfig config;
    private final String icebergJdbcUrl;
    private final String icebergJdbcUser;
    private final String icebergJdbcPassword;
    private volatile boolean done;

    public CatalogBootstrap(Gravitino gravitino, StreamhouseConfig config,
            @ConfigProperty(name = "streamhouse.iceberg-catalog.jdbc-url", defaultValue = "jdbc:postgresql://platform-db:5432/iceberg") String icebergJdbcUrl,
            @ConfigProperty(name = "streamhouse.iceberg-catalog.jdbc-user", defaultValue = "streamhouse") String icebergJdbcUser,
            @ConfigProperty(name = "streamhouse.iceberg-catalog.jdbc-password", defaultValue = "streamhouse") String icebergJdbcPassword) {
        this.gravitino = gravitino;
        this.config = config;
        this.icebergJdbcUrl = icebergJdbcUrl;
        this.icebergJdbcUser = icebergJdbcUser;
        this.icebergJdbcPassword = icebergJdbcPassword;
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
        } catch (ComponentException e) {
            LOG.warnf("Gravitino bootstrap pending: %s", e.getMessage());
        }
    }

    Map<String, Object> kafkaCatalog() {
        return Map.of("name", "kafka", "type", "MESSAGING", "provider", "kafka", "comment", "Kafka topics",
                "properties", Map.of("bootstrap.servers", config.internal().kafkaBootstrap()));
    }

    Map<String, Object> icebergCatalog() {
        StreamhouseConfig.Internal internal = config.internal();
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("catalog-backend", "jdbc");
        properties.put("catalog-backend-name", "lake");
        properties.put("uri", icebergJdbcUrl);
        properties.put("jdbc-driver", "org.postgresql.Driver");
        properties.put("jdbc-user", icebergJdbcUser);
        properties.put("jdbc-password", icebergJdbcPassword);
        properties.put("jdbc-initialize", "true");
        properties.put("warehouse", "s3://warehouse/");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3-endpoint", internal.s3Endpoint());
        properties.put("s3-region", internal.s3Region());
        properties.put("s3-access-key-id", internal.s3AccessKey());
        properties.put("s3-secret-access-key", internal.s3SecretKey());
        properties.put("s3-path-style-access", "true");
        return Map.of("name", "lake", "type", "RELATIONAL", "provider", "lakehouse-iceberg",
                "comment", "Iceberg tables written by Tableflow", "properties", properties);
    }
}

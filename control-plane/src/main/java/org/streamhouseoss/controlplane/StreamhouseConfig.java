package org.streamhouseoss.controlplane;

import java.time.Duration;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Control plane settings. Service endpoints are REST client settings
 * ({@code quarkus.rest-client.<service>.url}); this holds the rest.
 */
@ConfigMapping(prefix = "streamhouse")
public interface StreamhouseConfig {

    @WithDefault("streamhouse")
    String metalake();

    @WithDefault("5s")
    Duration reconcileInterval();

    Flink flink();

    Internal internal();

    Topics topics();

    Lake lake();

    interface Flink {
        /** Catalog exposing Kafka topics as tables (the flink-catalog module); statements run in it. */
        @WithDefault("streamhouse")
        String topicCatalog();

        @WithDefault("local")
        String topicDatabase();
    }

    /** Addresses rendered into connector configs; they are resolved inside the platform network. */
    interface Internal {
        String kafkaBootstrap();

        String apicurioUrl();
    }

    interface Topics {
        @WithDefault("1")
        int partitions();

        @WithDefault("1")
        short replicationFactor();
    }

    /**
     * The Iceberg catalog Flink writes to. Flink registers it itself (deploy/compose/flink/catalogs/lake.yaml);
     * these settings register the same tables in Gravitino for browsing.
     */
    interface Lake {
        @WithDefault("streamhouse")
        String namespace();

        @WithDefault("jdbc:postgresql://platform-db:5432/iceberg")
        String jdbcUrl();

        @WithDefault("streamhouse")
        String jdbcUser();

        @WithDefault("streamhouse")
        String jdbcPassword();

        @WithDefault("s3://warehouse/")
        String warehouse();

        String s3Endpoint();

        String s3AccessKey();

        String s3SecretKey();

        @WithDefault("us-east-1")
        String s3Region();
    }
}

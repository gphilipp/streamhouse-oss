package org.streamhouseoss.controlplane;

import java.time.Duration;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "streamhouse")
public interface StreamhouseConfig {

    /** Confluent-compatible schema registry API, as reached by the control plane. */
    String registryUrl();

    String connectUrl();

    String gravitinoUrl();

    String contextEngineUrl();

    Flink flink();

    Internal internal();

    Topics topics();

    @WithDefault("streamhouse")
    String metalake();

    /** Iceberg namespace that topics are materialized into. */
    @WithDefault("streamhouse")
    String icebergNamespace();

    @WithDefault("5s")
    Duration reconcileInterval();

    @WithDefault("_streamhouse.audit")
    String auditTopic();

    interface Flink {
        String gatewayUrl();

        String jobmanagerUrl();

        @WithDefault("10s")
        Duration checkpointInterval();
    }

    /** Addresses rendered into connector and Flink configs; they are resolved inside the platform network. */
    interface Internal {
        String kafkaBootstrap();

        String registryUrl();

        String apicurioUrl();

        String icebergRestUrl();

        String s3Endpoint();

        String s3AccessKey();

        String s3SecretKey();

        @WithDefault("us-east-1")
        String s3Region();
    }

    interface Topics {
        @WithDefault("1")
        int partitions();

        @WithDefault("1")
        short replicationFactor();
    }
}

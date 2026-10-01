package org.streamhouseoss.controlplane.flink;

import java.time.Duration;

import org.streamhouseoss.controlplane.StreamhouseConfig;

/** Configuration matching the local compose stack, for unit tests. */
public final class TestConfig {

    private TestConfig() {
    }

    public static StreamhouseConfig config() {
        return new StreamhouseConfig() {
            public String registryUrl() { return "http://localhost:8085/apis/ccompat/v7"; }
            public String connectUrl() { return "http://localhost:8083"; }
            public String gravitinoUrl() { return "http://localhost:8090"; }
            public String contextEngineUrl() { return "http://localhost:8082"; }
            public String metalake() { return "streamhouse"; }
            public String icebergNamespace() { return "streamhouse"; }
            public Duration reconcileInterval() { return Duration.ofSeconds(5); }
            public String auditTopic() { return "_streamhouse.audit"; }
            public Flink flink() {
                return new Flink() {
                    public String gatewayUrl() { return "http://localhost:8084"; }
                    public String jobmanagerUrl() { return "http://localhost:8081"; }
                    public Duration checkpointInterval() { return Duration.ofSeconds(10); }
                    public String topicCatalog() { return "streamhouse"; }
                    public String topicDatabase() { return "local"; }
                };
            }
            public Internal internal() {
                return new Internal() {
                    public String kafkaBootstrap() { return "kafka:9092"; }
                    public String registryUrl() { return "http://apicurio:8080/apis/ccompat/v7"; }
                    public String apicurioUrl() { return "http://apicurio:8080/apis/registry/v3"; }
                    public String icebergRestUrl() { return "http://gravitino:9001/iceberg/"; }
                    public String s3Endpoint() { return "http://s3:8333"; }
                    public String s3AccessKey() { return "streamhouse"; }
                    public String s3SecretKey() { return "streamhouse-secret"; }
                    public String s3Region() { return "us-east-1"; }
                };
            }
            public Topics topics() {
                return new Topics() {
                    public int partitions() { return 1; }
                    public short replicationFactor() { return 1; }
                };
            }
        };
    }
}

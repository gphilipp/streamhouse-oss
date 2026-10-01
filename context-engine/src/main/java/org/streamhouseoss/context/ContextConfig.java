package org.streamhouseoss.context;

import java.time.Duration;
import java.util.Set;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "streamhouse.context")
public interface ContextConfig {

    /** Base URL of a Confluent-compatible schema registry API. */
    String registryUrl();

    /** Upper bound for LIMIT in lightning queries. */
    @WithDefault("1000")
    int maxRows();

    /** LIMIT applied when a query has none. */
    @WithDefault("100")
    int defaultRows();

    @WithDefault("5s")
    Duration queryTimeout();

    /** Maximum records written to the serving store in one transaction. */
    @WithDefault("2000")
    int batchMaxRecords();

    /** Maximum time records wait before being flushed. Bounds end-to-end freshness. */
    @WithDefault("100ms")
    Duration batchMaxWait();

    /** Roles that may query every table without explicit grants. */
    @WithDefault("admin,engineer")
    Set<String> superRoles();

    /** Kafka topic receiving audit events; empty disables Kafka audit publishing. */
    @WithDefault("_streamhouse.audit")
    String auditTopic();
}

package org.streamhouseoss.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Desired-state resources managed by the control plane. Each resource is identified by its
 * {@link #kind()} and {@link #name()}; the control plane persists them and reconciles them
 * against Kafka, Kafka Connect, Flink, Gravitino and the context engine.
 */
public sealed interface Resource {

    ResourceKind kind();

    String name();

    /** A named connection to an external system, e.g. an OLTP database used for CDC. */
    record Connection(String name, ConnectionType type, Map<String, OptionValue> options) implements Resource {
        public Connection {
            requireName(name);
            Objects.requireNonNull(type, "type");
            options = Map.copyOf(options);
        }

        @Override
        public ResourceKind kind() {
            return ResourceKind.CONNECTION;
        }
    }

    /** A CDC source capturing tables from a connection, one topic per table. */
    record Source(String name, String connection, List<TableRef> tables) implements Resource {
        public Source {
            requireName(name);
            requireName(connection);
            if (tables.isEmpty()) {
                throw new IllegalArgumentException("source " + name + " must capture at least one table");
            }
            tables = List.copyOf(tables);
        }

        @Override
        public ResourceKind kind() {
            return ResourceKind.SOURCE;
        }

        /** The Kafka topic a captured table lands in. */
        public String topicFor(TableRef table) {
            return name + "." + table.schema() + "." + table.table();
        }
    }

    /** A continuously maintained Flink SQL query whose result is written to a compacted topic. */
    record MaterializedView(String name, List<String> primaryKey, String query) implements Resource {
        public MaterializedView {
            requireName(name);
            if (primaryKey.isEmpty()) {
                throw new IllegalArgumentException("materialized view " + name + " needs a PRIMARY KEY");
            }
            primaryKey = List.copyOf(primaryKey);
            Objects.requireNonNull(query, "query");
        }

        @Override
        public ResourceKind kind() {
            return ResourceKind.MATERIALIZED_VIEW;
        }
    }

    /** Continuous materialization of a topic into an Iceberg table. */
    record Tableflow(String name, TableMode mode) implements Resource {
        public Tableflow {
            requireTopicName(name);
            Objects.requireNonNull(mode, "mode");
        }

        @Override
        public ResourceKind kind() {
            return ResourceKind.TABLEFLOW;
        }
    }

    /**
     * Materialization of a topic into the context engine's serving store, queryable by apps
     * and agents. A {@code null} mode means "infer from the topic's cleanup.policy".
     */
    record ContextTable(String name, TableMode mode, String description) implements Resource {
        public ContextTable {
            requireTopicName(name);
            description = description == null ? "" : description;
        }

        @Override
        public ResourceKind kind() {
            return ResourceKind.CONTEXT_TABLE;
        }
    }

    /** {@code GRANT SELECT ON CONTEXT <table> TO ROLE <role>}. */
    record Grant(Privilege privilege, ResourceKind objectKind, String objectName, String role) implements Resource {
        public Grant {
            Objects.requireNonNull(privilege, "privilege");
            Objects.requireNonNull(objectKind, "objectKind");
            requireTopicName(objectName);
            requireName(role);
        }

        @Override
        public ResourceKind kind() {
            return ResourceKind.GRANT;
        }

        /** Grants are keyed by everything they say; granting twice is idempotent. */
        @Override
        public String name() {
            return privilege + ":" + objectKind + ":" + objectName + ":" + role;
        }
    }

    private static void requireName(String name) {
        if (name == null || !Names.IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid name: " + name);
        }
    }

    private static void requireTopicName(String name) {
        if (name == null || !Names.TOPIC.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid topic name: " + name);
        }
    }
}

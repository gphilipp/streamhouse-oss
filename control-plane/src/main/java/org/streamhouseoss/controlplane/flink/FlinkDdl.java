package org.streamhouseoss.controlplane.flink;

import java.util.List;
import java.util.stream.Collectors;

import org.streamhouseoss.controlplane.StreamhouseConfig;
import org.streamhouseoss.controlplane.clients.FlinkGateway.Column;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Flink SQL the control plane generates. Kafka topics are read and written through the topic
 * catalog (flink-catalog), which owns how a topic maps to a table; Iceberg tables live in the
 * {@code lake} catalog, registered in every Flink session by the catalog store.
 */
@ApplicationScoped
public class FlinkDdl {

    public static final String LAKE = "lake";

    private final String namespace;
    private final String topicCatalog;
    private final String topicDatabase;

    @Inject
    public FlinkDdl(StreamhouseConfig config) {
        this(config.lake().namespace(), config.flink().topicCatalog(), config.flink().topicDatabase());
    }

    public FlinkDdl(String namespace, String topicCatalog, String topicDatabase) {
        this.namespace = namespace;
        this.topicCatalog = topicCatalog;
        this.topicDatabase = topicDatabase;
    }

    public String namespace() {
        return namespace;
    }

    /** Statements that make the topic catalog current, so unqualified names are topics. */
    public List<String> useTopicCatalog() {
        return List.of("USE CATALOG " + quote(topicCatalog), "USE " + quote(topicDatabase));
    }

    /** A topic as a fully qualified table of the topic catalog. */
    public String topicTable(String topic) {
        return quote(topicCatalog) + "." + quote(topicDatabase) + "." + quote(topic);
    }

    /**
     * A materialized view is a CREATE TABLE ... AS SELECT into an upsert topic keyed by the
     * primary key, in the same dialect a statement would use.
     */
    public static String materializedView(String name, List<String> primaryKey, String query) {
        return "CREATE TABLE " + quote(name) + " (PRIMARY KEY (" + quoteAll(primaryKey) + ") NOT ENFORCED) "
                + "WITH ('changelog.mode' = 'upsert') AS " + query;
    }

    /**
     * An append-only view of a keyed topic: every record, updates included, using the topic
     * catalog's own schema and formats.
     */
    public String appendLog(String name, String topic, List<String> keyColumns, String groupId) {
        return "CREATE TEMPORARY TABLE " + quote(name) + " WITH ('connector' = 'kafka', "
                + "'scan.startup.mode' = 'earliest-offset', 'key.fields' = " + literal(String.join(";", keyColumns)) + ", "
                + "'properties.group.id' = " + literal(groupId) + ") "
                + "LIKE " + topicTable(topic) + " (EXCLUDING CONSTRAINTS OVERWRITING OPTIONS)";
    }

    public String icebergNamespace() {
        return "CREATE DATABASE IF NOT EXISTS " + LAKE + "." + quote(namespace);
    }

    /** Iceberg table for a topic; with a primary key it is a v2 upsert table (equality deletes). */
    public String icebergTable(String table, List<Column> columns, List<String> primaryKey) {
        String cols = columns.stream()
                .map(c -> quote(c.name()) + " " + c.type() + (c.nullable() && !primaryKey.contains(c.name()) ? "" : " NOT NULL"))
                .collect(Collectors.joining(", "));
        if (primaryKey.isEmpty()) {
            return "CREATE TABLE IF NOT EXISTS " + icebergTableName(table) + " (" + cols + ")";
        }
        return "CREATE TABLE IF NOT EXISTS " + icebergTableName(table) + " (" + cols + ", PRIMARY KEY ("
                + quoteAll(primaryKey) + ") NOT ENFORCED) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')";
    }

    public String icebergTableName(String table) {
        return LAKE + "." + quote(namespace) + "." + quote(table);
    }

    /** Iceberg table name for a topic: dots are not allowed in Iceberg identifiers here. */
    public static String icebergTableFor(String topic) {
        return topic.replace('.', '_');
    }

    public static String set(String key, String value) {
        return "SET " + literal(key) + " = " + literal(value);
    }

    public static String quote(String identifier) {
        return '`' + identifier.replace("`", "``") + '`';
    }

    public static String quoteAll(List<String> identifiers) {
        return identifiers.stream().map(FlinkDdl::quote).collect(Collectors.joining(", "));
    }

    static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}

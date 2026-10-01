package org.streamhouseoss.controlplane.flink;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.streamhouseoss.controlplane.StreamhouseConfig;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Renders Flink SQL DDL for the tables jobs read and write: Kafka topics (Avro in the Confluent
 * wire format, schemas in the registry) and Iceberg tables in the {@code lake} catalog.
 */
@ApplicationScoped
public class FlinkDdl {

    public static final String LAKE = "lake";

    /** A Flink column: name, SQL type and nullability. */
    public record FlinkColumn(String name, String type, boolean nullable) {
        String ddl(boolean forceNotNull) {
            return quote(name) + " " + type + (nullable && !forceNotNull ? "" : " NOT NULL");
        }
    }

    private final StreamhouseConfig.Internal internal;
    private final String namespace;

    public FlinkDdl(StreamhouseConfig config) {
        this.internal = config.internal();
        this.namespace = config.icebergNamespace();
    }

    // ---- Avro -> Flink types ------------------------------------------------------------------

    public static List<FlinkColumn> columns(Schema record) {
        if (record.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("expected an Avro record schema, got " + record.getType());
        }
        List<FlinkColumn> columns = new ArrayList<>();
        for (Schema.Field field : record.getFields()) {
            columns.add(new FlinkColumn(field.name(), type(field.schema()), nullable(field.schema())));
        }
        return columns;
    }

    static boolean nullable(Schema schema) {
        return schema.getType() == Schema.Type.UNION && schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }

    static String type(Schema schema) {
        Schema s = schema;
        if (s.getType() == Schema.Type.UNION) {
            List<Schema> branches = s.getTypes().stream().filter(b -> b.getType() != Schema.Type.NULL).toList();
            if (branches.size() != 1) {
                throw new IllegalArgumentException("unions other than [null, T] are not supported: " + schema);
            }
            s = branches.getFirst();
        }
        LogicalType logical = s.getLogicalType();
        return switch (s.getType()) {
            case BOOLEAN -> "BOOLEAN";
            case INT -> logical instanceof LogicalTypes.Date ? "DATE"
                    : logical instanceof LogicalTypes.TimeMillis ? "TIME(3)" : "INT";
            case LONG -> {
                if (logical instanceof LogicalTypes.TimestampMillis || logical instanceof LogicalTypes.LocalTimestampMillis) {
                    yield "TIMESTAMP(3)";
                }
                if (logical instanceof LogicalTypes.TimestampMicros || logical instanceof LogicalTypes.LocalTimestampMicros) {
                    yield "TIMESTAMP(6)";
                }
                yield "BIGINT";
            }
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case STRING, ENUM -> "STRING";
            case BYTES, FIXED -> logical instanceof LogicalTypes.Decimal d
                    ? "DECIMAL(" + d.getPrecision() + ", " + d.getScale() + ")" : "BYTES";
            case RECORD -> "ROW<" + s.getFields().stream()
                    .map(f -> quote(f.name()) + " " + type(f.schema()) + (nullable(f.schema()) ? "" : " NOT NULL"))
                    .collect(Collectors.joining(", ")) + ">";
            case ARRAY -> "ARRAY<" + type(s.getElementType()) + ">";
            case MAP -> "MAP<STRING, " + type(s.getValueType()) + ">";
            case UNION, NULL -> throw new IllegalArgumentException("unsupported Avro type " + s);
        };
    }

    // ---- DDL ----------------------------------------------------------------------------------

    /**
     * A temporary table over a topic, named after the topic. With key fields it is an
     * {@code upsert-kafka} changelog (latest value per key, tombstones delete); without, a plain
     * append-only {@code kafka} source read from the earliest offset.
     */
    public String kafkaSource(String topic, List<FlinkColumn> columns, List<String> keyFields, String groupId) {
        Map<String, String> options = new LinkedHashMap<>();
        String pk = "";
        if (!keyFields.isEmpty()) {
            options.put("connector", "upsert-kafka");
            options.put("key.format", "avro-confluent");
            options.put("key.avro-confluent.url", internal.registryUrl());
            options.put("value.fields-include", "ALL");
            pk = ", PRIMARY KEY (" + keyFields.stream().map(FlinkDdl::quote).collect(Collectors.joining(", ")) + ") NOT ENFORCED";
        } else {
            options.put("connector", "kafka");
            options.put("scan.startup.mode", "earliest-offset");
        }
        options.put("topic", topic);
        options.put("properties.bootstrap.servers", internal.kafkaBootstrap());
        options.put("properties.group.id", groupId);
        options.put("value.format", "avro-confluent");
        options.put("value.avro-confluent.url", internal.registryUrl());
        String cols = columns.stream().map(c -> c.ddl(keyFields.contains(c.name()))).collect(Collectors.joining(", "));
        return "CREATE TEMPORARY TABLE " + quote(topic) + " (" + cols + pk + ") WITH (" + options(options) + ")";
    }

    /** A temporary upsert-kafka sink writing Avro key/value and registering their schemas. */
    public String upsertKafkaSink(String tableName, String topic, List<FlinkColumn> columns, List<String> primaryKey) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", "upsert-kafka");
        options.put("topic", topic);
        options.put("properties.bootstrap.servers", internal.kafkaBootstrap());
        options.put("key.format", "avro-confluent");
        options.put("key.avro-confluent.url", internal.registryUrl());
        options.put("value.format", "avro-confluent");
        options.put("value.avro-confluent.url", internal.registryUrl());
        options.put("value.fields-include", "ALL");
        String cols = columns.stream().map(c -> c.ddl(primaryKey.contains(c.name()))).collect(Collectors.joining(", "));
        return "CREATE TEMPORARY TABLE " + quote(tableName) + " (" + cols + ", PRIMARY KEY ("
                + primaryKey.stream().map(FlinkDdl::quote).collect(Collectors.joining(", ")) + ") NOT ENFORCED) WITH ("
                + options(options) + ")";
    }

    public String icebergCatalog() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("type", "iceberg");
        options.put("catalog-type", "rest");
        options.put("uri", internal.icebergRestUrl());
        options.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        options.put("s3.endpoint", internal.s3Endpoint());
        options.put("s3.path-style-access", "true");
        options.put("s3.access-key-id", internal.s3AccessKey());
        options.put("s3.secret-access-key", internal.s3SecretKey());
        options.put("client.region", internal.s3Region());
        return "CREATE CATALOG " + LAKE + " WITH (" + options(options) + ")";
    }

    public String icebergNamespace() {
        return "CREATE DATABASE IF NOT EXISTS " + LAKE + "." + quote(namespace);
    }

    /** Iceberg table for a topic; with a primary key it is a v2 upsert table (equality deletes). */
    public String icebergTable(String table, List<FlinkColumn> columns, List<String> primaryKey) {
        String cols = columns.stream().map(c -> c.ddl(primaryKey.contains(c.name()))).collect(Collectors.joining(", "));
        if (primaryKey.isEmpty()) {
            return "CREATE TABLE IF NOT EXISTS " + icebergTableName(table) + " (" + cols + ")";
        }
        return "CREATE TABLE IF NOT EXISTS " + icebergTableName(table) + " (" + cols + ", PRIMARY KEY ("
                + primaryKey.stream().map(FlinkDdl::quote).collect(Collectors.joining(", "))
                + ") NOT ENFORCED) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')";
    }

    public String icebergTableName(String table) {
        return LAKE + "." + quote(namespace) + "." + quote(table);
    }

    public String namespace() {
        return namespace;
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

    static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String options(Map<String, String> options) {
        return options.entrySet().stream().map(e -> literal(e.getKey()) + " = " + literal(e.getValue()))
                .collect(Collectors.joining(", "));
    }
}

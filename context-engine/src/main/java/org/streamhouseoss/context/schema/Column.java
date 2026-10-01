package org.streamhouseoss.context.schema;

/**
 * A column of a materialized table. {@code doc} carries the Avro field documentation so agents
 * get it through {@code getMetadata}.
 */
public record Column(String name, ColumnType type, boolean nullable, String doc) {

    /** Columns maintained by the engine itself, prefixed with an underscore. */
    public static final Column KAFKA_TIMESTAMP = new Column("_timestamp", ColumnType.TIMESTAMPTZ, false,
            "Kafka record timestamp of the latest change");
    public static final Column KAFKA_PARTITION = new Column("_partition", ColumnType.INTEGER, false, "Kafka partition");
    public static final Column KAFKA_OFFSET = new Column("_offset", ColumnType.BIGINT, false, "Kafka offset");
    /** Used when a record key is not an Avro record (e.g. a plain string or integer). */
    public static final String SCALAR_KEY = "_key";

    public Column {
        doc = doc == null ? "" : doc;
    }

    public boolean system() {
        return name.startsWith("_");
    }
}

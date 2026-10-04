package org.streamhouseoss.flink.catalog;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.flink.table.types.DataType;

/**
 * A topic seen as a table, in the vocabulary of topics-as-tables Flink SQL: key and value
 * columns, key/value formats ({@code avro-registry}, {@code raw}, {@code avro-debezium-registry}),
 * changelog mode ({@code append}, {@code upsert}, {@code retract}). The value never repeats the
 * key columns, and tables are always read from the earliest offset.
 */
record TableSpec(
        List<Column> keyColumns,
        List<Column> valueColumns,
        String keyFormat,
        String valueFormat,
        String keyFieldsPrefix,
        String changelogMode) {

    static final String AVRO = "avro-registry";
    static final String AVRO_DEBEZIUM = "avro-debezium-registry";
    static final String RAW = "raw";
    static final String APPEND = "append";
    static final String UPSERT = "upsert";
    static final String RETRACT = "retract";
    /** The name of the single column of a raw (non-Avro) key. */
    static final String RAW_KEY_COLUMN = "key";

    record Column(String name, DataType type) {
    }

    TableSpec {
        keyColumns = List.copyOf(keyColumns);
        valueColumns = List.copyOf(valueColumns);
        keyFieldsPrefix = keyFieldsPrefix == null ? "" : keyFieldsPrefix;
    }

    /** Physical columns in table order: key columns, then value columns. */
    List<Column> physicalColumns() {
        return Stream.concat(keyColumns.stream(), valueColumns.stream()).toList();
    }

    Optional<String> keyFormatIfAny() {
        return keyColumns.isEmpty() ? Optional.empty() : Optional.ofNullable(keyFormat);
    }
}

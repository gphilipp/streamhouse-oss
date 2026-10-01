package org.streamhouseoss.flink.catalog;

import java.util.List;
import java.util.Optional;

import org.apache.flink.table.types.DataType;

/**
 * A topic seen as a table, in the vocabulary of topics-as-tables Flink SQL: key and value
 * columns, key/value formats ({@code avro-registry}, {@code raw}, {@code avro-debezium-registry}),
 * changelog mode ({@code append}, {@code upsert}, {@code retract}).
 */
record TableSpec(
        List<Column> keyColumns,
        List<Column> valueColumns,
        String keyFormat,
        String valueFormat,
        String keyFieldsPrefix,
        boolean valueIncludesKey,
        String changelogMode,
        String startupMode) {

    static final String AVRO = "avro-registry";
    static final String AVRO_DEBEZIUM = "avro-debezium-registry";
    static final String RAW = "raw";
    static final String APPEND = "append";
    static final String UPSERT = "upsert";
    static final String RETRACT = "retract";

    record Column(String name, DataType type) {
    }

    TableSpec {
        keyColumns = List.copyOf(keyColumns);
        valueColumns = List.copyOf(valueColumns);
        keyFieldsPrefix = keyFieldsPrefix == null ? "" : keyFieldsPrefix;
        startupMode = startupMode == null ? "earliest-offset" : startupMode;
    }

    /** Physical columns in table order: key columns first (unless included in the value), then value columns. */
    List<Column> physicalColumns() {
        if (valueIncludesKey) {
            return valueColumns;
        }
        return java.util.stream.Stream.concat(keyColumns.stream(), valueColumns.stream()).toList();
    }

    Optional<String> keyFormatIfAny() {
        return keyColumns.isEmpty() ? Optional.empty() : Optional.ofNullable(keyFormat);
    }

    TableSpec withChangelogMode(String mode) {
        return new TableSpec(keyColumns, valueColumns, keyFormat, valueFormat, keyFieldsPrefix, valueIncludesKey, mode, startupMode);
    }
}

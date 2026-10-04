package org.streamhouseoss.context.ingest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.streamhouseoss.context.schema.AvroColumns;
import org.streamhouseoss.context.schema.Column;
import org.streamhouseoss.context.schema.SchemaException;
import org.streamhouseoss.context.store.TableMode;

/** The columns and key of a serving table, derived from a topic's key and value schemas. */
record Layout(List<String> keyColumns, List<Column> columns) {

    Layout {
        keyColumns = List.copyOf(keyColumns);
        columns = List.copyOf(columns);
    }

    /**
     * Layout for one record. In upsert mode the key fields become the primary key and are added
     * as columns when the value does not already carry them; a non-record key becomes {@code _key}.
     */
    static Layout of(TableMode mode, Schema keySchema, Schema valueSchema) {
        if (valueSchema.getType() != Schema.Type.RECORD) {
            throw new SchemaException("value schema must be an Avro record, got " + valueSchema.getType());
        }
        Map<String, Column> columns = new LinkedHashMap<>();
        List<String> keys = new ArrayList<>();
        if (mode == TableMode.UPSERT) {
            for (Column keyColumn : AvroColumns.columns(keySchema, Column.SCALAR_KEY)) {
                keys.add(keyColumn.name());
                columns.put(keyColumn.name(), keyColumn);
            }
        }
        for (Column valueColumn : AvroColumns.columns(valueSchema, null)) {
            if (valueColumn.system()) {
                throw new SchemaException("field " + valueColumn.name() + " clashes with system columns (names starting with _)");
            }
            Column keyColumn = columns.get(valueColumn.name());
            if (keyColumn != null && keyColumn.type() != valueColumn.type()) {
                throw new SchemaException("key field " + keyColumn.name() + " is " + keyColumn.type()
                        + " but the value field is " + valueColumn.type());
            }
            columns.put(valueColumn.name(), keyColumn != null ? keyColumn : valueColumn);
        }
        if (mode == TableMode.APPEND) {
            columns.put(Column.KAFKA_PARTITION.name(), Column.KAFKA_PARTITION);
            columns.put(Column.KAFKA_OFFSET.name(), Column.KAFKA_OFFSET);
        }
        columns.put(Column.KAFKA_TIMESTAMP.name(), Column.KAFKA_TIMESTAMP);
        return new Layout(keys, new ArrayList<>(columns.values()));
    }

    /**
     * Merges a record's layout into this one: new columns are appended (schema evolution adds
     * nullable columns), removed fields keep their column. Key or type changes are incompatible.
     */
    Layout merge(Layout incoming) {
        if (columns.isEmpty()) {
            return incoming;
        }
        if (!keyColumns.equals(incoming.keyColumns)) {
            throw new SchemaException("incompatible schema change: key columns changed from " + keyColumns
                    + " to " + incoming.keyColumns);
        }
        Map<String, Column> merged = new LinkedHashMap<>();
        columns.forEach(c -> merged.put(c.name(), c));
        for (Column c : incoming.columns) {
            Column existing = merged.get(c.name());
            if (existing == null) {
                merged.put(c.name(), new Column(c.name(), c.type(), true, c.doc()));
            } else if (existing.type() != c.type()) {
                throw new SchemaException("incompatible schema change: column " + c.name() + " changed from "
                        + existing.type() + " to " + c.type());
            }
        }
        return merged.size() == columns.size() ? this : new Layout(keyColumns, new ArrayList<>(merged.values()));
    }
}

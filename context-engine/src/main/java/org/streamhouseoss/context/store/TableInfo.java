package org.streamhouseoss.context.store;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.streamhouseoss.context.schema.Column;
import org.streamhouseoss.model.TableMode;

/** Definition and state of one context table, as stored in {@code serving._tables}. */
public record TableInfo(
        String topic,
        TableMode mode,
        String description,
        List<String> keyColumns,
        List<Column> columns,
        TableStatus status,
        String statusMessage,
        OffsetDateTime lastRecordTimestamp,
        OffsetDateTime lastIngestedAt) {

    public TableInfo {
        keyColumns = List.copyOf(keyColumns);
        columns = List.copyOf(columns);
    }

    /** Whether the Postgres table exists (it is created lazily from the first record's schema). */
    public boolean materialized() {
        return !columns.isEmpty();
    }

    public Optional<Column> column(String name) {
        return columns.stream().filter(c -> c.name().equals(name)).findFirst();
    }
}

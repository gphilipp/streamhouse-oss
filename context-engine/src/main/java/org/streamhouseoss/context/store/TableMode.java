package org.streamhouseoss.context.store;

/** How a topic is materialized into a serving table. */
public enum TableMode {
    /** Every record becomes a row; suited to immutable event logs. */
    APPEND,
    /** Only the latest record per key is kept; tombstones delete. Suited to entity state. */
    UPSERT
}

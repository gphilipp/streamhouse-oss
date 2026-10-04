package org.streamhouseoss.model;

import java.util.Locale;

public enum ResourceKind {
    CONNECTION,
    SOURCE,
    MATERIALIZED_VIEW,
    STATEMENT,
    ICEBERG_TABLE,
    CONTEXT_TABLE,
    GRANT;

    /** Lower-case words for messages, e.g. "materialized view". */
    public String label() {
        return name().toLowerCase(Locale.ROOT).replace('_', ' ');
    }
}

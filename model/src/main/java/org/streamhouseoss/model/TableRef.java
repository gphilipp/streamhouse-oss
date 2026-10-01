package org.streamhouseoss.model;

/** A table in a source database, e.g. {@code public.orders}. */
public record TableRef(String schema, String table) {
    public TableRef {
        if (!Names.IDENTIFIER.matcher(schema).matches() || !Names.IDENTIFIER.matcher(table).matches()) {
            throw new IllegalArgumentException("invalid table reference: " + schema + "." + table);
        }
    }

    @Override
    public String toString() {
        return schema + "." + table;
    }
}

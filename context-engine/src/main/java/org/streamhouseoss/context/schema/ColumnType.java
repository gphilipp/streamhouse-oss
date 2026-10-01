package org.streamhouseoss.context.schema;

/** Postgres types the context engine materializes into. */
public enum ColumnType {
    BOOLEAN("boolean"),
    INTEGER("integer"),
    BIGINT("bigint"),
    REAL("real"),
    DOUBLE("double precision"),
    NUMERIC("numeric"),
    TEXT("text"),
    BYTES("bytea"),
    DATE("date"),
    TIME("time"),
    TIMESTAMP("timestamp"),
    TIMESTAMPTZ("timestamptz"),
    JSON("jsonb");

    private final String sqlType;

    ColumnType(String sqlType) {
        this.sqlType = sqlType;
    }

    public String sqlType() {
        return sqlType;
    }

    /** Placeholder used when binding a value of this type; JSON is bound as text and cast. */
    public String placeholder() {
        return this == JSON ? "CAST(? AS jsonb)" : "?";
    }

    public boolean comparable() {
        return this != JSON && this != BYTES;
    }
}

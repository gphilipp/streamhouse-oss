package org.streamhouseoss.context.schema;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

/**
 * Maps Avro schemas to Postgres columns and Avro values to JDBC values. Understands Avro logical
 * types, which is what Debezium emits with {@code time.precision.mode=connect}, plus Debezium's
 * string-encoded {@code ZonedTimestamp}.
 */
public final class AvroColumns {

    private static final Conversions.DecimalConversion DECIMALS = new Conversions.DecimalConversion();
    private static final String ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";

    private AvroColumns() {
    }

    /** Columns for the fields of a record schema, or a single column for a scalar schema. */
    public static List<Column> columns(Schema schema, String scalarName) {
        if (schema.getType() != Schema.Type.RECORD) {
            return List.of(new Column(scalarName, typeOf(schema), isNullable(schema), schema.getDoc()));
        }
        List<Column> columns = new ArrayList<>();
        for (Schema.Field field : schema.getFields()) {
            columns.add(new Column(field.name(), typeOf(field.schema()), isNullable(field.schema()), field.doc()));
        }
        return columns;
    }

    public static boolean isNullable(Schema schema) {
        return schema.getType() == Schema.Type.NULL
                || schema.getType() == Schema.Type.UNION && schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }

    /** The non-null branch of an optional union, or the schema itself. */
    static Schema unwrapOptional(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        List<Schema> branches = schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).toList();
        return branches.size() == 1 ? branches.getFirst() : schema;
    }

    public static ColumnType typeOf(Schema schema) {
        Schema s = unwrapOptional(schema);
        LogicalType logical = s.getLogicalType();
        return switch (s.getType()) {
            case BOOLEAN -> ColumnType.BOOLEAN;
            case INT -> logical instanceof LogicalTypes.Date ? ColumnType.DATE
                    : logical instanceof LogicalTypes.TimeMillis ? ColumnType.TIME
                    : ColumnType.INTEGER;
            case LONG -> {
                if (logical instanceof LogicalTypes.TimestampMillis || logical instanceof LogicalTypes.TimestampMicros
                        || logical instanceof LogicalTypes.TimestampNanos) {
                    yield ColumnType.TIMESTAMPTZ;
                }
                if (logical instanceof LogicalTypes.LocalTimestampMillis || logical instanceof LogicalTypes.LocalTimestampMicros
                        || logical instanceof LogicalTypes.LocalTimestampNanos) {
                    yield ColumnType.TIMESTAMP;
                }
                yield logical instanceof LogicalTypes.TimeMicros ? ColumnType.TIME : ColumnType.BIGINT;
            }
            case FLOAT -> ColumnType.REAL;
            case DOUBLE -> ColumnType.DOUBLE;
            // Debezium renders timestamptz as an ISO-8601 string even with time.precision.mode=connect.
            case STRING -> ZONED_TIMESTAMP.equals(s.getProp("connect.name")) ? ColumnType.TIMESTAMPTZ : ColumnType.TEXT;
            case ENUM -> ColumnType.TEXT;
            case BYTES, FIXED -> logical instanceof LogicalTypes.Decimal ? ColumnType.NUMERIC : ColumnType.BYTES;
            case RECORD, ARRAY, MAP, UNION -> ColumnType.JSON;
            case NULL -> ColumnType.TEXT;
        };
    }

    /** Converts an Avro value to the JDBC value bound for a column of {@link #typeOf(Schema)}. */
    public static Object toJdbc(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        Schema s = unwrapOptional(schema);
        return switch (typeOf(s)) {
            case BOOLEAN, INTEGER, BIGINT, REAL, DOUBLE -> value;
            case TEXT -> value.toString();
            case BYTES -> bytes(value);
            case NUMERIC -> DECIMALS.fromBytes(ByteBuffer.wrap(bytes(value)), s, s.getLogicalType());
            case DATE -> LocalDate.ofEpochDay(((Number) value).longValue());
            case TIME -> s.getType() == Schema.Type.INT
                    ? LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L)
                    : LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000L);
            case TIMESTAMPTZ -> s.getType() == Schema.Type.STRING
                    ? OffsetDateTime.parse(value.toString())
                    : OffsetDateTime.ofInstant(instant(s.getLogicalType(), ((Number) value).longValue()), ZoneOffset.UTC);
            case TIMESTAMP -> LocalDateTime.ofInstant(instant(s.getLogicalType(), ((Number) value).longValue()), ZoneOffset.UTC);
            case JSON -> GenericData.get().toString(value);
        };
    }

    private static Instant instant(LogicalType logical, long v) {
        if (logical instanceof LogicalTypes.TimestampMicros || logical instanceof LogicalTypes.LocalTimestampMicros) {
            return Instant.ofEpochSecond(Math.floorDiv(v, 1_000_000L), Math.floorMod(v, 1_000_000L) * 1_000L);
        }
        if (logical instanceof LogicalTypes.TimestampNanos || logical instanceof LogicalTypes.LocalTimestampNanos) {
            return Instant.ofEpochSecond(Math.floorDiv(v, 1_000_000_000L), Math.floorMod(v, 1_000_000_000L));
        }
        return Instant.ofEpochMilli(v);
    }

    private static byte[] bytes(Object value) {
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
            byte[] out = new byte[copy.remaining()];
            copy.get(out);
            return out;
        }
        if (value instanceof GenericFixed fixed) {
            return fixed.bytes();
        }
        return (byte[]) value;
    }
}

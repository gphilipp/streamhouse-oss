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
 * types plus the Kafka Connect / Debezium semantic types that the Avro converter leaves in
 * {@code connect.name}.
 */
public final class AvroColumns {

    private static final Conversions.DecimalConversion DECIMALS = new Conversions.DecimalConversion();

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
        String connectName = s.getProp("connect.name");
        return switch (s.getType()) {
            case BOOLEAN -> ColumnType.BOOLEAN;
            case INT -> {
                if (logical instanceof LogicalTypes.Date || "io.debezium.time.Date".equals(connectName)
                        || "org.apache.kafka.connect.data.Date".equals(connectName)) {
                    yield ColumnType.DATE;
                }
                if (logical instanceof LogicalTypes.TimeMillis || "io.debezium.time.Time".equals(connectName)) {
                    yield ColumnType.TIME;
                }
                yield ColumnType.INTEGER;
            }
            case LONG -> {
                if (logical instanceof LogicalTypes.TimestampMillis || logical instanceof LogicalTypes.TimestampMicros
                        || logical instanceof LogicalTypes.TimestampNanos
                        || "org.apache.kafka.connect.data.Timestamp".equals(connectName)) {
                    yield ColumnType.TIMESTAMPTZ;
                }
                if (logical instanceof LogicalTypes.LocalTimestampMillis || logical instanceof LogicalTypes.LocalTimestampMicros
                        || logical instanceof LogicalTypes.LocalTimestampNanos
                        || "io.debezium.time.Timestamp".equals(connectName)
                        || "io.debezium.time.MicroTimestamp".equals(connectName)
                        || "io.debezium.time.NanoTimestamp".equals(connectName)) {
                    yield ColumnType.TIMESTAMP;
                }
                if (logical instanceof LogicalTypes.TimeMicros || "io.debezium.time.MicroTime".equals(connectName)) {
                    yield ColumnType.TIME;
                }
                yield ColumnType.BIGINT;
            }
            case FLOAT -> ColumnType.REAL;
            case DOUBLE -> ColumnType.DOUBLE;
            case STRING -> "io.debezium.time.ZonedTimestamp".equals(connectName) ? ColumnType.TIMESTAMPTZ : ColumnType.TEXT;
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
        ColumnType type = typeOf(s);
        String connectName = s.getProp("connect.name");
        return switch (type) {
            case BOOLEAN, INTEGER, BIGINT, REAL, DOUBLE -> value;
            case TEXT -> value.toString();
            case BYTES -> bytes(value);
            case NUMERIC -> DECIMALS.fromBytes(ByteBuffer.wrap(bytes(value)), s, s.getLogicalType());
            case DATE -> LocalDate.ofEpochDay(((Number) value).longValue());
            case TIME -> s.getType() == Schema.Type.INT
                    ? LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L)
                    : LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000L);
            case TIMESTAMPTZ -> {
                if (s.getType() == Schema.Type.STRING) {
                    yield OffsetDateTime.parse(value.toString());
                }
                yield OffsetDateTime.ofInstant(instant(s.getLogicalType(), connectName, ((Number) value).longValue()), ZoneOffset.UTC);
            }
            case TIMESTAMP -> LocalDateTime.ofInstant(instant(s.getLogicalType(), connectName, ((Number) value).longValue()), ZoneOffset.UTC);
            case JSON -> GenericData.get().toString(value);
        };
    }

    private static Instant instant(LogicalType logical, String connectName, long v) {
        if (logical instanceof LogicalTypes.TimestampMicros || logical instanceof LogicalTypes.LocalTimestampMicros
                || "io.debezium.time.MicroTimestamp".equals(connectName)) {
            return Instant.ofEpochSecond(Math.floorDiv(v, 1_000_000L), Math.floorMod(v, 1_000_000L) * 1_000L);
        }
        if (logical instanceof LogicalTypes.TimestampNanos || logical instanceof LogicalTypes.LocalTimestampNanos
                || "io.debezium.time.NanoTimestamp".equals(connectName)) {
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

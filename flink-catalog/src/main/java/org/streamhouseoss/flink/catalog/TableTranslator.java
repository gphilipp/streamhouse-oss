package org.streamhouseoss.flink.catalog;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Translation between the topics-as-tables model and Apache Flink's Kafka connectors.
 *
 * <ul>
 * <li>Inference (existing topics): value fields from the {@code <topic>-value} Avro schema; key
 * fields from an Avro record {@code <topic>-key}, otherwise one raw {@code key} column (STRING when
 * {@code <topic>-key} is the primitive {@code "string"}, BYTES otherwise); key columns get the
 * {@code key_} prefix when their names collide with value fields; compacted topics are
 * {@code upsert}, Debezium envelopes {@code retract}, everything else {@code append}.</li>
 * <li>Creation (CREATE TABLE / CTAS): Confluent-style options are validated and turned into a
 * {@link TableSpec} plus topic settings (partitions from {@code DISTRIBUTED BY ... INTO n BUCKETS},
 * cleanup policy). Everything inference needs later lives in the registry and the topic config: a
 * raw key is recorded as a primitive schema under {@code <topic>-key}.</li>
 * <li>Rendering: a {@link TableSpec} becomes a Flink {@code kafka}/{@code upsert-kafka}
 * {@link CatalogTable} with a {@code $rowtime} column and a 180 ms bounded-out-of-orderness
 * watermark.</li>
 * </ul>
 */
final class TableTranslator {

    static final String ROWTIME = "$rowtime";
    static final String KEY_PREFIX = "key_";

    /** Options accepted in CREATE TABLE ... WITH (...). */
    static final Set<String> SUPPORTED_OPTIONS = Set.of("connector", "changelog.mode", "key.format", "value.format",
            "kafka.cleanup-policy");

    /** Kafka topic settings derived from a CREATE TABLE. */
    record TopicSettings(int partitions, String cleanupPolicy) {
    }

    /** The result of translating a CREATE TABLE. */
    record Creation(TableSpec spec, TopicSettings topic) {
    }

    private TableTranslator() {
    }

    // ---- inference ----------------------------------------------------------------------------

    /**
     * Infers the table for an existing topic.
     *
     * @param keySchema the {@code <topic>-key} Avro schema, if registered
     * @param valueSchema the {@code <topic>-value} Avro schema
     * @param compacted whether the topic's cleanup.policy includes compact
     */
    static TableSpec infer(Optional<String> keySchema, String valueSchema, boolean compacted) {
        DataType value = AvroSchemaConverter.convertToDataType(valueSchema);
        if (!(value.getLogicalType() instanceof RowType)) {
            throw new CatalogException("the value schema must be an Avro record");
        }
        List<String> valueNames = DataType.getFieldNames(value);
        int after = valueNames.indexOf("after");
        if (after >= 0 && valueNames.contains("before") && valueNames.contains("op")) {
            DataType afterType = DataType.getFieldDataTypes(value).get(after);
            // Debezium envelope: the table is the "after" image, read as a changelog.
            return new TableSpec(List.of(), columns(afterType.nullable()), null, TableSpec.AVRO_DEBEZIUM, "", TableSpec.RETRACT);
        }
        List<TableSpec.Column> valueColumns = columns(value);
        List<TableSpec.Column> keyColumns;
        String keyFormat;
        DataType key = keySchema.map(AvroSchemaConverter::convertToDataType).orElse(null);
        if (key != null && key.getLogicalType() instanceof RowType) {
            keyColumns = columns(key).stream().map(c -> new TableSpec.Column(c.name(), c.type().notNull())).toList();
            keyFormat = TableSpec.AVRO;
        } else {
            boolean string = key != null && isString(key.getLogicalType());
            keyColumns = List.of(new TableSpec.Column(TableSpec.RAW_KEY_COLUMN, string ? DataTypes.STRING() : DataTypes.BYTES()));
            keyFormat = TableSpec.RAW;
        }
        Set<String> valueNameSet = new HashSet<>(valueNames);
        boolean collision = keyColumns.stream().anyMatch(c -> valueNameSet.contains(c.name()));
        String prefix = collision ? KEY_PREFIX : "";
        if (collision) {
            keyColumns = keyColumns.stream().map(c -> new TableSpec.Column(KEY_PREFIX + c.name(), c.type())).toList();
        }
        if (compacted && keyFormat.equals(TableSpec.RAW)) {
            keyColumns = keyColumns.stream().map(c -> new TableSpec.Column(c.name(), c.type().notNull())).toList();
        }
        return new TableSpec(keyColumns, valueColumns, keyFormat, TableSpec.AVRO, prefix,
                compacted ? TableSpec.UPSERT : TableSpec.APPEND);
    }

    private static List<TableSpec.Column> columns(DataType row) {
        List<String> names = DataType.getFieldNames(row);
        List<DataType> types = DataType.getFieldDataTypes(row);
        List<TableSpec.Column> columns = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            columns.add(new TableSpec.Column(names.get(i), types.get(i)));
        }
        return columns;
    }

    // ---- creation -----------------------------------------------------------------------------

    static Creation fromCreate(ResolvedCatalogTable table, int defaultPartitions) {
        Map<String, String> options = new LinkedHashMap<>(table.getOptions());
        List<String> unsupported = options.keySet().stream().filter(k -> !SUPPORTED_OPTIONS.contains(k)).sorted().toList();
        if (!unsupported.isEmpty()) {
            throw new CatalogException("unsupported table option(s) " + unsupported + "; supported: "
                    + SUPPORTED_OPTIONS.stream().sorted().toList());
        }
        String connector = options.getOrDefault("connector", "confluent");
        if (!connector.equals("confluent")) {
            throw new CatalogException("tables in this catalog are Kafka topics; 'connector' cannot be '" + connector + "'");
        }
        ResolvedSchema schema = table.getResolvedSchema();
        List<TableSpec.Column> physical = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            if (column.getName().equals(ROWTIME)) {
                continue;
            }
            if (!column.isPhysical()) {
                throw new CatalogException("column " + column.getName() + " is computed or metadata; only physical columns "
                        + "are stored in the topic");
            }
            physical.add(new TableSpec.Column(column.getName(), column.getDataType()));
        }
        List<String> primaryKey = schema.getPrimaryKey().map(pk -> pk.getColumns()).orElse(List.of());
        Optional<TableDistribution> distribution = table.getDistribution();
        List<String> keyNames = !primaryKey.isEmpty() ? primaryKey
                : distribution.map(TableDistribution::getBucketKeys).orElse(List.of());

        String mode = options.getOrDefault("changelog.mode", primaryKey.isEmpty() ? TableSpec.APPEND : TableSpec.UPSERT)
                .toLowerCase(Locale.ROOT);
        switch (mode) {
            case TableSpec.UPSERT -> {
                if (primaryKey.isEmpty()) {
                    throw new CatalogException("'changelog.mode' = 'upsert' requires a PRIMARY KEY");
                }
            }
            case TableSpec.APPEND -> {
            }
            case TableSpec.RETRACT -> throw new CatalogException(
                    "'changelog.mode' = 'retract' is not supported for new tables; declare a PRIMARY KEY and use 'upsert'");
            default -> throw new CatalogException("'changelog.mode' must be 'append' or 'upsert', got '" + mode + "'");
        }

        String keyFormat = keyNames.isEmpty() ? null : format(options.getOrDefault("key.format", TableSpec.AVRO), "key.format");
        String valueFormat = format(options.getOrDefault("value.format", TableSpec.AVRO), "value.format");
        if (valueFormat.equals(TableSpec.RAW)) {
            throw new CatalogException("'value.format' = 'raw' is not supported; use 'avro-registry'");
        }

        List<TableSpec.Column> keyColumns = new ArrayList<>();
        for (String name : keyNames) {
            TableSpec.Column column = physical.stream().filter(c -> c.name().equals(name)).findFirst()
                    .orElseThrow(() -> new CatalogException("key column " + name + " is not a physical column"));
            keyColumns.add(new TableSpec.Column(name, column.type().notNull()));
        }
        if (TableSpec.RAW.equals(keyFormat)) {
            if (keyColumns.size() != 1 || !keyColumns.getFirst().name().equals(TableSpec.RAW_KEY_COLUMN)
                    || !isStringOrBytes(keyColumns.getFirst().type().getLogicalType())) {
                throw new CatalogException("'key.format' = 'raw' needs exactly one key column, named `"
                        + TableSpec.RAW_KEY_COLUMN + "`, of type STRING or BYTES");
            }
        }
        // The value never repeats the key (Confluent's default), so inference finds the same columns.
        List<TableSpec.Column> valueColumns = physical.stream().filter(c -> !keyNames.contains(c.name())).toList();
        TableSpec spec = new TableSpec(keyColumns, valueColumns, keyFormat, valueFormat, "", mode);

        int partitions = distribution.flatMap(TableDistribution::getBucketCount).orElse(defaultPartitions);
        String cleanup = options.getOrDefault("kafka.cleanup-policy", mode.equals(TableSpec.UPSERT) ? "compact" : "delete")
                .toLowerCase(Locale.ROOT);
        String cleanupPolicy = switch (cleanup) {
            case "delete", "compact" -> cleanup;
            case "delete-compact" -> "compact,delete";
            default -> throw new CatalogException("'kafka.cleanup-policy' must be delete, compact or delete-compact");
        };
        return new Creation(spec, new TopicSettings(partitions, cleanupPolicy));
    }

    private static boolean isString(LogicalType type) {
        return type.getTypeRoot() == LogicalTypeRoot.VARCHAR || type.getTypeRoot() == LogicalTypeRoot.CHAR;
    }

    private static boolean isStringOrBytes(LogicalType type) {
        return isString(type) || type.getTypeRoot() == LogicalTypeRoot.VARBINARY || type.getTypeRoot() == LogicalTypeRoot.BINARY;
    }

    private static String format(String value, String option) {
        String v = value.toLowerCase(Locale.ROOT);
        if (v.equals(TableSpec.AVRO) || v.equals(TableSpec.RAW)) {
            return v;
        }
        throw new CatalogException("'" + option + "' must be 'avro-registry' or 'raw', got '" + value + "'");
    }

    /** The Avro schemas to pre-register for a new table, keyed by subject suffix ("key", "value"). */
    static Map<String, String> avroSchemas(TableSpec spec) {
        Map<String, String> schemas = new LinkedHashMap<>();
        if (!spec.keyColumns().isEmpty() && TableSpec.AVRO.equals(spec.keyFormat())) {
            schemas.put("key", avroSchema(row(spec.keyColumns())));
        } else if (!spec.keyColumns().isEmpty()) {
            // Records the raw key's type for inference; Flink's raw format writes the bytes unframed.
            schemas.put("key", isString(spec.keyColumns().getFirst().type().getLogicalType()) ? "\"string\"" : "\"bytes\"");
        }
        schemas.put("value", avroSchema(row(spec.valueColumns())));
        return schemas;
    }

    private static RowType row(List<TableSpec.Column> columns) {
        return (RowType) DataTypes.ROW(columns.stream().map(c -> DataTypes.FIELD(c.name(), c.type())).toList())
                .notNull().getLogicalType();
    }

    /**
     * The Avro schema Flink's avro-confluent format writes for a row type. Called reflectively
     * because the format jar in the Flink image relocates Avro, which changes the return type.
     */
    static String avroSchema(RowType row) {
        try {
            Method convert = AvroSchemaConverter.class.getMethod("convertToSchema", LogicalType.class);
            return convert.invoke(null, row).toString();
        } catch (ReflectiveOperationException e) {
            throw new CatalogException("cannot derive Avro schema", e);
        }
    }

    // ---- rendering ----------------------------------------------------------------------------

    /** A Flink Kafka table for the topic, ready for the kafka / upsert-kafka connectors. */
    static CatalogTable toCatalogTable(String topic, TableSpec spec, String bootstrapServers, String registryUrl,
            Map<String, String> kafkaProperties) {
        Schema.Builder schema = Schema.newBuilder();
        for (TableSpec.Column column : spec.physicalColumns()) {
            schema.column(column.name(), column.type());
        }
        schema.columnByMetadata(ROWTIME, DataTypes.TIMESTAMP_LTZ(3), "timestamp", true);
        schema.watermark(ROWTIME, "`" + ROWTIME + "` - INTERVAL '0.180' SECOND");
        List<String> keyNames = spec.keyColumns().stream().map(TableSpec.Column::name).toList();

        Map<String, String> options = new LinkedHashMap<>();
        if (spec.changelogMode().equals(TableSpec.UPSERT)) {
            if (keyNames.isEmpty()) {
                throw new CatalogException("topic " + topic + " is compacted but has no key to upsert on");
            }
            options.put("connector", "upsert-kafka");
            schema.primaryKey(keyNames);
        } else {
            options.put("connector", "kafka");
            options.put("scan.startup.mode", "earliest-offset");
            if (!keyNames.isEmpty()) {
                options.put("key.fields", String.join(";", keyNames));
            }
        }
        options.put("topic", topic);
        options.put("properties.bootstrap.servers", bootstrapServers);
        kafkaProperties.forEach((k, v) -> options.put("properties." + k, v));
        spec.keyFormatIfAny().ifPresent(format -> {
            String flinkFormat = flinkFormat(format);
            options.put("key.format", flinkFormat);
            if (!flinkFormat.equals("raw")) {
                options.put("key." + flinkFormat + ".url", registryUrl);
            }
            options.put("value.fields-include", "EXCEPT_KEY");
            if (!spec.keyFieldsPrefix().isEmpty()) {
                options.put("key.fields-prefix", spec.keyFieldsPrefix());
            }
        });
        String valueFormat = flinkFormat(spec.valueFormat());
        options.put("value.format", valueFormat);
        options.put("value." + valueFormat + ".url", registryUrl);
        return CatalogTable.newBuilder()
                .schema(schema.build())
                .comment("Kafka topic " + topic + " (" + spec.changelogMode() + ")")
                .options(options)
                .build();
    }

    static String flinkFormat(String format) {
        return switch (format) {
            case TableSpec.AVRO -> "avro-confluent";
            case TableSpec.AVRO_DEBEZIUM -> "debezium-avro-confluent";
            case TableSpec.RAW -> "raw";
            default -> throw new CatalogException("unsupported format " + format);
        };
    }
}

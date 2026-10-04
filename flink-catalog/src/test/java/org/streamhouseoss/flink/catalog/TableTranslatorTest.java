package org.streamhouseoss.flink.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.jupiter.api.Test;

class TableTranslatorTest {

    private static final String ORDERS_KEY = """
            {"type":"record","name":"Key","namespace":"shop.public.orders","fields":[{"name":"order_id","type":"int"}]}""";
    private static final String ORDERS_VALUE = """
            {"type":"record","name":"Value","namespace":"shop.public.orders","fields":[
              {"name":"order_id","type":"int"},
              {"name":"status","type":"string"},
              {"name":"total","type":["null","double"],"default":null}]}""";
    private static final String DEBEZIUM_ENVELOPE = """
            {"type":"record","name":"Envelope","fields":[
              {"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}],"default":null},
              {"name":"after","type":["null","Value"],"default":null},
              {"name":"op","type":"string"},
              {"name":"ts_ms","type":["null","long"],"default":null}]}""";

    @Test
    void flattenedCdcTopicPrefixesCollidingKeyColumns() {
        TableSpec spec = TableTranslator.infer(Optional.of(ORDERS_KEY), ORDERS_VALUE, false);

        assertThat(spec.keyColumns()).extracting(TableSpec.Column::name).containsExactly("key_order_id");
        assertThat(spec.keyFieldsPrefix()).isEqualTo("key_");
        assertThat(spec.physicalColumns()).extracting(TableSpec.Column::name)
                .containsExactly("key_order_id", "order_id", "status", "total");
        assertThat(spec.changelogMode()).isEqualTo("append");

        CatalogTable table = TableTranslator.toCatalogTable("shop.public.orders", spec, "kafka:9092", "http://reg", Map.of());
        assertThat(table.getOptions()).containsEntry("connector", "kafka")
                .containsEntry("scan.startup.mode", "earliest-offset")
                .containsEntry("key.fields", "key_order_id")
                .containsEntry("key.fields-prefix", "key_")
                .containsEntry("key.format", "avro-confluent")
                .containsEntry("key.avro-confluent.url", "http://reg")
                .containsEntry("value.fields-include", "EXCEPT_KEY")
                .containsEntry("value.format", "avro-confluent");
        assertThat(table.getUnresolvedSchema().getColumns()).extracting(Schema.UnresolvedColumn::getName).contains("$rowtime");
        assertThat(table.getUnresolvedSchema().getWatermarkSpecs()).hasSize(1);
    }

    @Test
    void compactedTopicsAreUpsertTablesKeyedOnTheKafkaKey() {
        TableSpec spec = TableTranslator.infer(Optional.of(ORDERS_KEY), ORDERS_VALUE, true);
        CatalogTable table = TableTranslator.toCatalogTable("shop.public.orders", spec, "kafka:9092", "http://reg",
                Map.of("security.protocol", "SASL_PLAINTEXT"));

        assertThat(spec.changelogMode()).isEqualTo("upsert");
        assertThat(table.getOptions()).containsEntry("connector", "upsert-kafka")
                .containsEntry("properties.security.protocol", "SASL_PLAINTEXT")
                .doesNotContainKeys("key.fields", "scan.startup.mode");
        assertThat(table.getUnresolvedSchema().getPrimaryKey()).get()
                .extracting(Schema.UnresolvedPrimaryKey::getColumnNames).isEqualTo(List.of("key_order_id"));
    }

    @Test
    void topicsWithoutAKeySchemaGetARawKeyColumn() {
        TableSpec spec = TableTranslator.infer(Optional.empty(), ORDERS_VALUE, false);

        assertThat(spec.keyColumns()).containsExactly(new TableSpec.Column("key", DataTypes.BYTES()));
        assertThat(spec.keyFormat()).isEqualTo("raw");
        assertThat(TableTranslator.toCatalogTable("t", spec, "k", "r", Map.of()).getOptions())
                .containsEntry("key.format", "raw").doesNotContainKey("key.raw.url");
    }

    @Test
    void debeziumEnvelopesAreReadAsChangelogs() {
        TableSpec spec = TableTranslator.infer(Optional.of(ORDERS_KEY), DEBEZIUM_ENVELOPE, true);

        assertThat(spec.changelogMode()).isEqualTo("retract");
        assertThat(spec.physicalColumns()).extracting(TableSpec.Column::name).containsExactly("id", "name");
        assertThat(TableTranslator.toCatalogTable("t", spec, "k", "http://reg", Map.of()).getOptions())
                .containsEntry("connector", "kafka")
                .containsEntry("value.format", "debezium-avro-confluent")
                .containsEntry("value.debezium-avro-confluent.url", "http://reg");
    }

    @Test
    void ctasWithRawStringKeyRoundTripsThroughTheRegistry() {
        ResolvedCatalogTable created = resolved(
                List.of(Column.physical("key", DataTypes.STRING().notNull()), Column.physical("open_orders", DataTypes.BIGINT().notNull())),
                List.of("key"),
                Map.of("changelog.mode", "upsert", "key.format", "raw"),
                TableDistribution.ofHash(List.of("key"), 1));

        TableTranslator.Creation creation = TableTranslator.fromCreate(created, 6);

        assertThat(creation.topic()).isEqualTo(new TableTranslator.TopicSettings(1, "compact"));
        TableSpec spec = creation.spec();
        assertThat(spec.keyColumns()).extracting(TableSpec.Column::name).containsExactly("key");
        assertThat(spec.valueColumns()).extracting(TableSpec.Column::name).containsExactly("open_orders");
        Map<String, String> schemas = TableTranslator.avroSchemas(spec);
        assertThat(schemas.get("key")).isEqualTo("\"string\"");
        assertThat(schemas.get("value")).contains("\"open_orders\"").doesNotContain("\"key\"");

        // A later session sees the same table, inferred from the registered schemas and the compacted topic.
        TableSpec inferred = TableTranslator.infer(Optional.of(schemas.get("key")), schemas.get("value"), true);
        assertThat(inferred.keyColumns()).containsExactly(new TableSpec.Column("key", DataTypes.STRING().notNull()));
        assertThat(inferred.physicalColumns()).extracting(TableSpec.Column::name).containsExactly("key", "open_orders");
        CatalogTable table = TableTranslator.toCatalogTable("open_orders_by_customer", inferred, "k", "r", Map.of());
        assertThat(table.getOptions()).containsEntry("connector", "upsert-kafka").containsEntry("key.format", "raw")
                .containsEntry("value.fields-include", "EXCEPT_KEY");
        assertThat(table.getUnresolvedSchema().getPrimaryKey()).get()
                .extracting(Schema.UnresolvedPrimaryKey::getColumnNames).isEqualTo(List.of("key"));
    }

    @Test
    void createDefaultsMatchTopicsAsTablesSemantics() {
        ResolvedCatalogTable appendTable = resolved(List.of(Column.physical("s", DataTypes.STRING())), List.of(), Map.of(), null);
        TableTranslator.Creation append = TableTranslator.fromCreate(appendTable, 6);
        assertThat(append.topic()).isEqualTo(new TableTranslator.TopicSettings(6, "delete"));
        assertThat(append.spec().changelogMode()).isEqualTo("append");
        assertThat(append.spec().keyColumns()).isEmpty();

        ResolvedCatalogTable pkTable = resolved(
                List.of(Column.physical("id", DataTypes.INT().notNull()), Column.physical("s", DataTypes.STRING())),
                List.of("id"), Map.of("kafka.cleanup-policy", "delete-compact"), null);
        TableTranslator.Creation upsert = TableTranslator.fromCreate(pkTable, 6);
        assertThat(upsert.spec().changelogMode()).isEqualTo("upsert");
        assertThat(upsert.spec().keyFormat()).isEqualTo("avro-registry");
        assertThat(upsert.topic()).isEqualTo(new TableTranslator.TopicSettings(6, "compact,delete"));
        assertThat(TableTranslator.avroSchemas(upsert.spec())).containsOnlyKeys("key", "value");
    }

    @Test
    void rejectsWhatCannotBeHonoured() {
        List<Column> idAndName = List.of(Column.physical("id", DataTypes.INT().notNull()), Column.physical("s", DataTypes.STRING()));
        assertThatThrownBy(() -> TableTranslator.fromCreate(resolved(idAndName, List.of(), Map.of("changelog.mode", "upsert"), null), 6))
                .isInstanceOf(CatalogException.class).hasMessageContaining("requires a PRIMARY KEY");
        assertThatThrownBy(() -> TableTranslator.fromCreate(resolved(idAndName, List.of("id"), Map.of("changelog.mode", "retract"), null), 6))
                .hasMessageContaining("retract");
        assertThatThrownBy(() -> TableTranslator.fromCreate(resolved(idAndName, List.of("id"), Map.of("key.format", "raw"), null), 6))
                .hasMessageContaining("exactly one key column, named `key`, of type STRING or BYTES");
        assertThatThrownBy(() -> TableTranslator.fromCreate(resolved(idAndName, List.of(), Map.of("kafka.retention.time", "7 d"), null), 6))
                .hasMessageContaining("unsupported table option(s) [kafka.retention.time]");
        assertThatThrownBy(() -> TableTranslator.fromCreate(resolved(idAndName, List.of(), Map.of("error-handling.mode", "log"), null), 6))
                .hasMessageContaining("unsupported table option(s) [error-handling.mode]");
        assertThatThrownBy(() -> TableTranslator.fromCreate(resolved(idAndName, List.of(), Map.of("connector", "datagen"), null), 6))
                .hasMessageContaining("cannot be 'datagen'");
    }

    private static ResolvedCatalogTable resolved(List<Column> columns, List<String> primaryKey, Map<String, String> options,
            TableDistribution distribution) {
        ResolvedSchema schema = new ResolvedSchema(columns, List.of(),
                primaryKey.isEmpty() ? null : UniqueConstraint.primaryKey("pk", primaryKey));
        CatalogTable.Builder builder = CatalogTable.newBuilder().schema(Schema.newBuilder().fromResolvedSchema(schema).build())
                .options(options);
        if (distribution != null) {
            builder.distribution(distribution);
        }
        return new ResolvedCatalogTable(builder.build(), schema);
    }
}

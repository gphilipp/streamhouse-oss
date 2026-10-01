package org.streamhouseoss.controlplane.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.streamhouseoss.controlplane.flink.FlinkDdl.FlinkColumn;

class FlinkDdlTest {

    /** The value schema Debezium registers for shop.public.orders with the platform's converter settings. */
    private static final Schema DEBEZIUM_ORDERS = new Schema.Parser().parse("""
            {"type": "record", "name": "Value", "namespace": "shop.public.orders", "fields": [
              {"name": "order_id", "type": "int"},
              {"name": "customer_id", "type": "int"},
              {"name": "status", "type": "string"},
              {"name": "total", "type": "double"},
              {"name": "created_at", "type": {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"}},
              {"name": "note", "type": ["null", "string"], "default": null},
              {"name": "shipped_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
              {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
              {"name": "tags", "type": {"type": "array", "items": "string"}},
              {"name": "address", "type": ["null", {"type": "record", "name": "Address", "fields": [
                {"name": "city", "type": "string"}, {"name": "zip", "type": ["null", "string"]}]}]}
            ]}""");

    @Test
    void mapsAvroToFlinkTypes() {
        assertThat(FlinkDdl.columns(DEBEZIUM_ORDERS)).containsExactly(
                new FlinkColumn("order_id", "INT", false),
                new FlinkColumn("customer_id", "INT", false),
                new FlinkColumn("status", "STRING", false),
                new FlinkColumn("total", "DOUBLE", false),
                new FlinkColumn("created_at", "STRING", false),
                new FlinkColumn("note", "STRING", true),
                new FlinkColumn("shipped_at", "TIMESTAMP(3)", true),
                new FlinkColumn("amount", "DECIMAL(12, 2)", false),
                new FlinkColumn("tags", "ARRAY<STRING>", false),
                new FlinkColumn("address", "ROW<`city` STRING NOT NULL, `zip` STRING>", true));
    }

    @Test
    void upsertKafkaSourceUsesKeyFieldsAsPrimaryKey() {
        FlinkDdl ddl = new FlinkDdl(TestConfig.config());
        String sql = ddl.kafkaSource("shop.public.orders",
                List.of(new FlinkColumn("order_id", "INT", true), new FlinkColumn("status", "STRING", false)),
                List.of("order_id"), "mv-x-1");

        assertThat(sql).isEqualTo("CREATE TEMPORARY TABLE `shop.public.orders` (`order_id` INT NOT NULL, `status` STRING NOT NULL, "
                + "PRIMARY KEY (`order_id`) NOT ENFORCED) WITH ('connector' = 'upsert-kafka', 'key.format' = 'avro-confluent', "
                + "'key.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7', 'value.fields-include' = 'ALL', "
                + "'topic' = 'shop.public.orders', 'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'mv-x-1', "
                + "'value.format' = 'avro-confluent', 'value.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7')");
    }

    @Test
    void appendSourceReadsFromEarliest() {
        String sql = new FlinkDdl(TestConfig.config()).kafkaSource("clicks", List.of(new FlinkColumn("url", "STRING", true)), List.of(), "g");

        assertThat(sql).contains("'connector' = 'kafka'", "'scan.startup.mode' = 'earliest-offset'").doesNotContain("PRIMARY KEY");
    }

    @Test
    void icebergUpsertTablesAreFormatV2() {
        FlinkDdl ddl = new FlinkDdl(TestConfig.config());

        assertThat(ddl.icebergTable("customer_360", List.of(new FlinkColumn("customer_id", "INT", true)), List.of("customer_id")))
                .isEqualTo("CREATE TABLE IF NOT EXISTS lake.`streamhouse`.`customer_360` (`customer_id` INT NOT NULL, "
                        + "PRIMARY KEY (`customer_id`) NOT ENFORCED) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')");
        assertThat(FlinkDdl.icebergTableFor("shop.public.orders")).isEqualTo("shop_public_orders");
    }

    @Test
    void quotesIdentifiersAndLiterals() {
        assertThat(FlinkDdl.quote("we`ird")).isEqualTo("`we``ird`");
        assertThat(FlinkDdl.set("pipeline.name", "it's")).isEqualTo("SET 'pipeline.name' = 'it''s'");
    }
}

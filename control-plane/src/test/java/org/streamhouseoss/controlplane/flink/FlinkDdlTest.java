package org.streamhouseoss.controlplane.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.streamhouseoss.controlplane.clients.FlinkGateway.Column;

class FlinkDdlTest {

    private final FlinkDdl ddl = new FlinkDdl("streamhouse", "streamhouse", "local");

    @Test
    void materializedViewIsAnUpsertCtas() {
        assertThat(FlinkDdl.materializedView("customer_360", List.of("customer_id"), "SELECT 1 AS customer_id"))
                .isEqualTo("CREATE TABLE `customer_360` (PRIMARY KEY (`customer_id`) NOT ENFORCED) "
                        + "WITH ('changelog.mode' = 'upsert') AS SELECT 1 AS customer_id");
    }

    @Test
    void appendLogReusesTheCatalogTable() {
        assertThat(ddl.appendLog("__log", "shop.public.orders", List.of("key_order_id"), "job-1"))
                .isEqualTo("CREATE TEMPORARY TABLE `__log` WITH ('connector' = 'kafka', 'scan.startup.mode' = 'earliest-offset', "
                        + "'key.fields' = 'key_order_id', 'properties.group.id' = 'job-1') "
                        + "LIKE `streamhouse`.`local`.`shop.public.orders` (EXCLUDING CONSTRAINTS OVERWRITING OPTIONS)");
    }

    @Test
    void icebergUpsertTablesAreFormatV2() {
        assertThat(ddl.icebergTable("customer_360", List.of(new Column("customer_id", "INT", true)), List.of("customer_id")))
                .isEqualTo("CREATE TABLE IF NOT EXISTS lake.`streamhouse`.`customer_360` (`customer_id` INT NOT NULL, "
                        + "PRIMARY KEY (`customer_id`) NOT ENFORCED) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')");
        assertThat(ddl.icebergTable("clicks", List.of(new Column("url", "STRING", true)), List.of()))
                .isEqualTo("CREATE TABLE IF NOT EXISTS lake.`streamhouse`.`clicks` (`url` STRING)");
        assertThat(FlinkDdl.icebergTableFor("shop.public.orders")).isEqualTo("shop_public_orders");
    }

    @Test
    void quotesIdentifiersAndLiterals() {
        assertThat(FlinkDdl.quote("we`ird")).isEqualTo("`we``ird`");
        assertThat(FlinkDdl.set("pipeline.name", "it's")).isEqualTo("SET 'pipeline.name' = 'it''s'");
    }
}

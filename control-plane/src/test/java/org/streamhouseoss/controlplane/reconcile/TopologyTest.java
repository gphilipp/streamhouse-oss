package org.streamhouseoss.controlplane.reconcile;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.streamhouseoss.controlplane.state.Phase;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.TableMode;
import org.streamhouseoss.model.TableRef;

class TopologyTest {

    private static StoredResource stored(Resource r, boolean deleted) {
        return new StoredResource(r, "", 1, deleted, "test", null, Phase.READY, "", 1, Map.of(), null);
    }

    private final Topology topology = Topology.of(List.of(
            stored(new Resource.Source("shop", "pg",
                    List.of(new TableRef("public", "orders"), new TableRef("public", "orders_archive"))), false),
            stored(new Resource.MaterializedView("order_stats", List.of("status"),
                    "SELECT status, COUNT(*) AS n FROM `shop.public.orders` GROUP BY status"), false),
            stored(new Resource.IcebergTable("order_stats", TableMode.UPSERT), false),
            stored(new Resource.MaterializedView("dropped_view", List.of("id"), "SELECT 1 AS id FROM order_stats"), true)));

    @Test
    void producedTopicsIgnoreDeletedResources() {
        assertThat(topology.producedTopics()).containsExactly("shop.public.orders", "shop.public.orders_archive", "order_stats");
    }

    @Test
    void referencesMatchWholeTopicNamesOnly() {
        assertThat(topology.referencedTopics("SELECT * FROM `shop.public.orders`", null)).containsExactly("shop.public.orders");
        assertThat(topology.referencedTopics("SELECT * FROM shop.public.orders_archive a", null))
                .containsExactly("shop.public.orders_archive");
        assertThat(topology.referencedTopics("SELECT * FROM order_stats JOIN `shop.public.orders` o ON true", "order_stats"))
                .containsExactly("shop.public.orders");
        assertThat(topology.referencedTopics("SELECT order_stats_total FROM elsewhere", null)).isEmpty();
    }

    @Test
    void consumersOfATopic() {
        assertThat(topology.consumersOf("shop.public.orders")).extracting(StoredResource::name).containsExactly("order_stats");
        assertThat(topology.consumersOf("order_stats")).extracting(r -> r.kind().name()).containsExactly("ICEBERG_TABLE");
    }
}

package org.streamhouseoss.controlplane.reconcile;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StatementReconcilerTest {

    @Test
    void findsTheTableAStatementWrites() {
        assertThat(StatementReconciler.target("CREATE TABLE customer_360 (PRIMARY KEY (k) NOT ENFORCED) AS SELECT 1"))
                .contains("customer_360");
        assertThat(StatementReconciler.target("create table if not exists `streamhouse`.`local`.`orders.v2` as select 1"))
                .contains("orders.v2");
        assertThat(StatementReconciler.target("INSERT INTO `shop.public.audit` SELECT * FROM x")).contains("shop.public.audit");
        assertThat(StatementReconciler.target("ALTER TABLE `shop.public.orders` SET ('changelog.mode' = 'upsert')")).isEmpty();
    }

    @Test
    void extractsTheQueryOfACtas() {
        String ctas = """
                CREATE TABLE customer_360 (PRIMARY KEY (customer_key) NOT ENFORCED)
                DISTRIBUTED BY HASH(customer_key) INTO 1 BUCKETS
                WITH ('changelog.mode' = 'upsert', 'note' = 'not AS here')
                AS SELECT CAST(c.customer_id AS STRING) AS customer_key FROM `shop.public.customers` AS c""";
        assertThat(StatementReconciler.ctasQuery(ctas))
                .contains("SELECT CAST(c.customer_id AS STRING) AS customer_key FROM `shop.public.customers` AS c");
        assertThat(StatementReconciler.ctasQuery("ALTER TABLE t SET ('a' = 'b')")).isEmpty();
        assertThat(StatementReconciler.ctasQuery("CREATE TABLE t (id INT)")).isEmpty();
    }
}

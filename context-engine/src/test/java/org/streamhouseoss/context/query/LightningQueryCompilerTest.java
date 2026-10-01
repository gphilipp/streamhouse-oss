package org.streamhouseoss.context.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.streamhouseoss.context.schema.Column;
import org.streamhouseoss.context.schema.ColumnType;
import org.streamhouseoss.context.store.TableInfo;
import org.streamhouseoss.context.store.TableStatus;
import org.streamhouseoss.model.TableMode;

class LightningQueryCompilerTest {

    private static final TableInfo CUSTOMERS = new TableInfo("customer_360", TableMode.UPSERT, "Live per-customer state",
            List.of("customer_id"),
            List.of(new Column("customer_id", ColumnType.BIGINT, false, ""),
                    new Column("name", ColumnType.TEXT, true, ""),
                    new Column("lifetimeValue", ColumnType.NUMERIC, true, ""),
                    new Column("vip", ColumnType.BOOLEAN, true, ""),
                    new Column("last_order_at", ColumnType.TIMESTAMPTZ, true, ""),
                    new Column("tags", ColumnType.JSON, true, ""),
                    Column.KAFKA_TIMESTAMP),
            TableStatus.ACTIVE, null, null, null);

    private static final TableInfo ORDERS = new TableInfo("shop.public.orders", TableMode.UPSERT, "", List.of("id"),
            List.of(new Column("id", ColumnType.INTEGER, false, ""), Column.KAFKA_TIMESTAMP),
            TableStatus.ACTIVE, null, null, null);

    private final LightningQueryCompiler compiler = new LightningQueryCompiler(1000, 100);

    private LightningQueryCompiler.Compiled compile(String sql) {
        var parsed = compiler.parse(sql, List.of(CUSTOMERS.topic(), ORDERS.topic()));
        return compiler.compile(parsed, parsed.table().equals(CUSTOMERS.topic()) ? CUSTOMERS : ORDERS);
    }

    @Test
    void pointLookupBindsLiteralsAsParameters() {
        var q = compile("SELECT * FROM customer_360 WHERE customer_id = 42");

        assertThat(q.sql()).isEqualTo("SELECT \"customer_id\", \"name\", \"lifetimeValue\", \"vip\", \"last_order_at\", \"tags\" "
                + "FROM serving.\"customer_360\" WHERE \"customer_id\" = CAST(? AS bigint) LIMIT 101");
        assertThat(q.parameters()).containsExactly("42");
        assertThat(q.limit()).isEqualTo(100);
    }

    @Test
    void rangeScanWithCompoundPredicatesOrderAndLimit() {
        var q = compile("""
                select c.name, LifetimeValue as ltv from customer_360 c
                where (c.vip is true or lifetimeValue between 100 and 2000.5) and not name like 'A%'
                  and last_order_at >= TIMESTAMP '2026-09-01 00:00:00' and customer_id not in (1, -2)
                order by ltv desc, name nulls last
                limit 10 offset 5;""");

        assertThat(q.sql()).isEqualTo("SELECT \"name\", \"lifetimeValue\" AS \"ltv\" FROM serving.\"customer_360\" "
                + "WHERE ((((\"vip\" IS TRUE OR \"lifetimeValue\" BETWEEN CAST(? AS numeric) AND CAST(? AS numeric)) "
                + "AND NOT (\"name\" LIKE ?)) AND \"last_order_at\" >= CAST(? AS timestamptz)) "
                + "AND \"customer_id\" NOT IN (CAST(? AS bigint), CAST(? AS bigint))) "
                + "ORDER BY \"ltv\" DESC, \"name\" NULLS LAST LIMIT 11 OFFSET 5");
        assertThat(q.parameters()).containsExactly("100", "2000.5", "A%", "2026-09-01 00:00:00", "1", "-2");
    }

    @Test
    void dottedTopicNamesQuotedOrNot() {
        assertThat(compile("SELECT id FROM shop.public.orders WHERE 5 < id").sql())
                .isEqualTo("SELECT \"id\" FROM serving.\"shop.public.orders\" WHERE \"id\" > CAST(? AS integer) LIMIT 101");
        assertThat(compile("SELECT id FROM \"shop.public.orders\"").table()).isEqualTo("shop.public.orders");
    }

    @Test
    void aggregatesWithoutGroupBy() {
        var q = compile("SELECT COUNT(*), SUM(lifetimeValue) AS total FROM customer_360 WHERE vip = true");

        assertThat(q.aggregate()).isTrue();
        assertThat(q.sql()).isEqualTo("SELECT COUNT(*) AS \"count\", SUM(\"lifetimeValue\") AS \"total\" "
                + "FROM serving.\"customer_360\" WHERE \"vip\" = CAST(? AS boolean)");
        assertThat(q.parameters()).containsExactly("true");
    }

    @Test
    void injectionAttemptsStayParameters() {
        var q = compile("SELECT name FROM customer_360 WHERE name = 'x''; DROP TABLE serving._tables; --'");

        assertThat(q.sql()).doesNotContain("DROP");
        assertThat(q.parameters()).containsExactly("x'; DROP TABLE serving._tables; --");
    }

    @Test
    void rejectsUnsupportedConstructs() {
        assertRejected("SELECT * FROM customer_360 c JOIN shop.public.orders o ON o.id = c.customer_id", "joins");
        assertRejected("SELECT * FROM (SELECT * FROM customer_360)", "joins, subqueries");
        assertRejected("SELECT name, COUNT(*) FROM customer_360 GROUP BY name", "GROUP BY");
        assertRejected("SELECT name, COUNT(*) FROM customer_360", "cannot be mixed");
        assertRejected("SELECT * FROM customer_360 WHERE customer_id IN (SELECT id FROM shop.public.orders)", "literal");
        assertRejected("SELECT * FROM customer_360 WHERE upper(name) = 'A'", "column");
        assertRejected("SELECT * FROM customer_360 WHERE nme = 'A'", "unknown column nme");
        assertRejected("SELECT * FROM customer_360 WHERE tags = '[]'", "cannot be compared");
        assertRejected("SELECT * FROM customer_360 WHERE name = NULL", "IS NULL");
        assertRejected("SELECT * FROM customer_360 LIMIT 5000", "at most 1000");
        assertRejected("DELETE FROM customer_360", "only single-table SELECT");
        assertRejected("SELECT * FROM customer_360; DROP TABLE x", "syntax error");
        assertRejected("SELECT pg_sleep(10) FROM customer_360", "unsupported select item");
    }

    @Test
    void unknownTablesLookLikeForbiddenOnes() {
        assertThatThrownBy(() -> compiler.parse("SELECT * FROM secret_salaries", List.of("customer_360")))
                .isInstanceOfSatisfying(QueryException.class, e -> assertThat(e.reason()).isEqualTo(QueryException.Reason.NOT_FOUND))
                .hasMessage("table secret_salaries does not exist or you are not allowed to query it");
    }

    private void assertRejected(String sql, String messagePart) {
        assertThatThrownBy(() -> compile(sql)).as(sql)
                .isInstanceOf(QueryException.class)
                .hasMessageContaining(messagePart);
    }
}

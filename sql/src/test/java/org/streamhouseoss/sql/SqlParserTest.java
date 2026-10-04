package org.streamhouseoss.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.streamhouseoss.model.ConnectionType;
import org.streamhouseoss.model.OptionValue;
import org.streamhouseoss.model.Privilege;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.model.TableMode;
import org.streamhouseoss.model.TableRef;

class SqlParserTest {

    @Test
    void createConnectionWithSecret() {
        var stmt = (Statement.Apply) SqlParser.parseStatement("""
                CREATE CONNECTION shop_pg TYPE POSTGRES WITH (
                  host = 'shop-db', port = 5432, database = 'shop', user = 'debezium', password = SECRET 'shop_pg_pwd'
                )""");

        assertThat(stmt.orReplace()).isFalse();
        assertThat(stmt.resource()).isEqualTo(new Resource.Connection("shop_pg", ConnectionType.POSTGRES, Map.of(
                "host", new OptionValue.Literal("shop-db"),
                "port", new OptionValue.Literal("5432"),
                "database", new OptionValue.Literal("shop"),
                "user", new OptionValue.Literal("debezium"),
                "password", new OptionValue.Secret("shop_pg_pwd"))));
    }

    @Test
    void createSourceNormalizesIdentifiers() {
        var stmt = (Statement.Apply) SqlParser.parseStatement(
                "create or replace source Shop from connection shop_pg tables (public.Customers, public.orders);");

        assertThat(stmt.orReplace()).isTrue();
        var source = (Resource.Source) stmt.resource();
        assertThat(source).isEqualTo(new Resource.Source("shop", "shop_pg",
                List.of(new TableRef("public", "customers"), new TableRef("public", "orders"))));
        assertThat(source.topicFor(source.tables().get(1))).isEqualTo("shop.public.orders");
    }

    @Test
    void materializedViewKeepsQueryTextVerbatim() {
        String query = """
                SELECT c.id AS customer_id, c.name, COUNT(o.id) AS orders, SUM(o.total) AS lifetime_value
                FROM `shop.public.customers` c
                LEFT JOIN `shop.public.orders` o ON o.customer_id = c.id AND o.status <> 'cancelled; really'
                GROUP BY c.id, c.name""";
        List<Statement> stmts = SqlParser.parseScript(
                "CREATE MATERIALIZED VIEW customer_360 PRIMARY KEY (customer_id) AS " + query + ";\nSHOW TOPICS;");

        assertThat(stmts).hasSize(2);
        var mv = (Resource.MaterializedView) ((Statement.Apply) stmts.get(0)).resource();
        assertThat(mv.name()).isEqualTo("customer_360");
        assertThat(mv.primaryKey()).containsExactly("customer_id");
        assertThat(mv.query()).isEqualTo(query);
        assertThat(stmts.get(1)).isEqualTo(new Statement.Show(null, "SHOW TOPICS"));
    }

    @Test
    void alterTopicEnableAndDisable() {
        List<Statement> stmts = SqlParser.parseScript("""
                -- the Iceberg mode is inferred from the topic unless given
                ALTER TOPIC shop.public.orders ENABLE ICEBERG;
                ALTER TOPIC clicks ENABLE ICEBERG WITH (mode = 'append');
                ALTER TOPIC customer_360 ENABLE CONTEXT WITH (description = 'Live per-customer state');
                ALTER TOPIC clicks ENABLE CONTEXT WITH (mode = 'append');
                ALTER TOPIC clicks DISABLE CONTEXT;
                """);

        assertThat(stmts).extracting(s -> s instanceof Statement.Apply a ? a.resource() : s).containsExactly(
                new Resource.IcebergTable("shop.public.orders", null),
                new Resource.IcebergTable("clicks", TableMode.APPEND),
                new Resource.ContextTable("customer_360", null, "Live per-customer state"),
                new Resource.ContextTable("clicks", TableMode.APPEND, ""),
                new Statement.Remove(ResourceKind.CONTEXT_TABLE, "clicks", true, "ALTER TOPIC clicks DISABLE CONTEXT"));
    }

    @Test
    void statementsKeepFlinkSqlVerbatim() {
        String sql = """
                CREATE TABLE customer_360 (PRIMARY KEY (customer_key) NOT ENFORCED)
                DISTRIBUTED BY HASH(customer_key) INTO 1 BUCKETS WITH ('key.format' = 'raw')
                AS SELECT CAST(customer_id AS STRING) AS customer_key FROM `shop.public.customers`""";
        List<Statement> stmts = SqlParser.parseScript("CREATE STATEMENT `customer-360` AS " + sql
                + ";\nCREATE STATEMENT `orders.as-upsert` AS ALTER TABLE `shop.public.orders` SET ('changelog.mode' = 'upsert');"
                + "\nDROP STATEMENT IF EXISTS `customer-360`;\nSHOW STATEMENTS");

        assertThat(((Statement.Apply) stmts.get(0)).resource()).isEqualTo(new Resource.Statement("customer-360", sql));
        assertThat(stmts.get(2)).isEqualTo(new Statement.Remove(ResourceKind.STATEMENT, "customer-360", true,
                "DROP STATEMENT IF EXISTS `customer-360`"));
        assertThat(stmts.get(3)).isEqualTo(new Statement.Show(ResourceKind.STATEMENT, "SHOW STATEMENTS"));
        assertThatThrownBy(() -> SqlParser.parseStatement("CREATE STATEMENT `Bad_Name` AS SELECT 1"))
                .hasMessageContaining("invalid statement name: Bad_Name");
    }

    @Test
    void grantAndRevoke() {
        List<Statement> stmts = SqlParser.parseScript("""
                GRANT SELECT ON CONTEXT customer_360 TO ROLE support_agent;
                REVOKE SELECT ON CONTEXT customer_360 FROM ROLE support_agent;
                """);

        var grant = new Resource.Grant(Privilege.SELECT, ResourceKind.CONTEXT_TABLE, "customer_360", "support_agent");
        assertThat(((Statement.Apply) stmts.get(0)).resource()).isEqualTo(grant);
        assertThat(stmts.get(1)).isInstanceOfSatisfying(Statement.Remove.class, r -> {
            assertThat(r.kind()).isEqualTo(ResourceKind.GRANT);
            assertThat(r.name()).isEqualTo(grant.name());
        });
    }

    @Test
    void dropShowDescribe() {
        List<Statement> stmts = SqlParser.parseScript("""
                DROP MATERIALIZED VIEW IF EXISTS customer_360;
                DROP SOURCE shop;
                SHOW CONTEXT TABLES;
                SHOW MATERIALIZED VIEWS;
                DESCRIBE CONTEXT shop.public.orders;
                DESCRIBE SOURCE shop
                """);

        assertThat(stmts).containsExactly(
                new Statement.Remove(ResourceKind.MATERIALIZED_VIEW, "customer_360", true, "DROP MATERIALIZED VIEW IF EXISTS customer_360"),
                new Statement.Remove(ResourceKind.SOURCE, "shop", false, "DROP SOURCE shop"),
                new Statement.Show(ResourceKind.CONTEXT_TABLE, "SHOW CONTEXT TABLES"),
                new Statement.Show(ResourceKind.MATERIALIZED_VIEW, "SHOW MATERIALIZED VIEWS"),
                new Statement.Describe(ResourceKind.CONTEXT_TABLE, "shop.public.orders", "DESCRIBE CONTEXT shop.public.orders"),
                new Statement.Describe(ResourceKind.SOURCE, "shop", "DESCRIBE SOURCE shop"));
    }

    @Test
    void errorsReportLineAndToken() {
        assertThatThrownBy(() -> SqlParser.parseScript("SHOW TOPICS;\nCREATE SOURCE s FROM shop_pg TABLES (public.t)"))
                .isInstanceOf(SqlParseException.class)
                .hasMessage("line 2: syntax error at \"shop_pg\"; expected CONNECTION");
        assertThatThrownBy(() -> SqlParser.parseStatement("ALTER TOPIC t ENABLE ICEBERG WITH (mode = 'merge')"))
                .hasMessageContaining("mode must be 'append' or 'upsert'");
        assertThatThrownBy(() -> SqlParser.parseStatement("ALTER TOPIC t ENABLE CONTEXT WITH (colour = 'red')"))
                .hasMessageContaining("unknown option colour");
        assertThatThrownBy(() -> SqlParser.parseStatement("CREATE MATERIALIZED VIEW v PRIMARY KEY (id) AS ;"))
                .hasMessageContaining("expected a Flink SQL statement after AS");
        assertThatThrownBy(() -> SqlParser.parseStatement("CREATE SOURCE `Bad-Name` FROM CONNECTION c TABLES (public.t)"))
                .hasMessageContaining("invalid name: Bad-Name");
        assertThatThrownBy(() -> SqlParser.parseStatement("SELECT 1"))
                .hasMessageContaining("expected CREATE, DROP, ALTER TOPIC");
        assertThatThrownBy(() -> SqlParser.parseStatement("DROP SOURCE 42"))
                .hasMessage("line 1: syntax error at \"42\"; expected a name");
        assertThatThrownBy(() -> SqlParser.parseStatement("CREATE SOURCE s FROM CONNECTION c TABLES (orders)"))
                .hasMessageContaining("expected schema.table, got orders");
        assertThatThrownBy(() -> SqlParser.parseStatement("CREATE CONNECTION c TYPE MYSQL"))
                .hasMessageContaining("unsupported connection type mysql");
        assertThatThrownBy(() -> SqlParser.parseStatement("SHOW TOPICS; SHOW SOURCES"))
                .hasMessageContaining("exactly one statement");
    }

    @Test
    void embeddedFlinkSqlKeepsCommentsTabsAndSemicolonsInStrings() {
        String query = "SELECT\tid, -- the key\n  'a;b' AS s /* not the end; */ FROM `t`";
        List<Statement> stmts = SqlParser.parseScript(
                "CREATE STATEMENT `copy` AS " + query + " ;\n\tSHOW STATEMENTS");

        assertThat(((Resource.Statement) ((Statement.Apply) stmts.get(0)).resource()).sql()).isEqualTo(query);
        assertThat(stmts.get(1).text()).isEqualTo("SHOW STATEMENTS");
    }

    @Test
    void reservedWordsWorkAsOptionNamesAndKindWordsAsNames() {
        var connection = (Resource.Connection) ((Statement.Apply) SqlParser.parseStatement(
                "CREATE CONNECTION source TYPE postgres WITH (user = 'u', 'key.format' = 'raw', publication = 'p', ssl = TRUE)")).resource();

        assertThat(connection.name()).isEqualTo("source");
        assertThat(connection.options()).containsEntry("user", new OptionValue.Literal("u"))
                .containsEntry("key.format", new OptionValue.Literal("raw"))
                .containsEntry("ssl", new OptionValue.Literal("true"));
        assertThat(SqlParser.parseStatement("DROP SOURCE if")).isEqualTo(
                new Statement.Remove(ResourceKind.SOURCE, "if", false, "DROP SOURCE if"));
    }
}

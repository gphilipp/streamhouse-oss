package org.streamhouseoss.controlplane.api;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.streamhouseoss.controlplane.reconcile.ReconcileLoop;

import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;

/**
 * The SQL API against a real desired-state database. The reconcile loop is mocked: these tests
 * cover what is accepted, rejected and recorded, not how it is provisioned (see e2e/).
 */
@QuarkusTest
@QuarkusTestResource(value = StateResource.class, restrictToAnnotatedClass = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SqlApiTest {

    private static final String CONNECTION = """
            CREATE CONNECTION shop_pg TYPE POSTGRES WITH (host = 'shop-db', database = 'shop', user = 'debezium',
              password = SECRET 'shop_pg_pwd')""";
    private static final String SOURCE = "CREATE SOURCE shop FROM CONNECTION shop_pg TABLES (public.customers, public.orders)";
    private static final String VIEW = """
            CREATE MATERIALIZED VIEW open_orders PRIMARY KEY (customer_id) AS
            SELECT customer_id, COUNT(*) AS n FROM `shop.public.orders` WHERE status = 'open' GROUP BY customer_id""";

    @InjectMock
    ReconcileLoop loop;

    private static JsonPath sql(String script) {
        return given().contentType(ContentType.JSON).body(Map.of("sql", script))
                .post("/v1/sql").then().statusCode(200).extract().jsonPath();
    }

    @Test
    @Order(1)
    @TestSecurity(user = "eng", roles = "engineer")
    void appliesAScriptInOrder() {
        JsonPath result = sql(CONNECTION + ";\n" + SOURCE + ";\n" + VIEW + ";\n"
                + "ALTER TOPIC open_orders ENABLE CONTEXT WITH (description = 'Open orders per customer');\n"
                + "ALTER TOPIC shop.public.orders ENABLE TABLEFLOW WITH (mode = 'append');");

        assertThat(result.getBoolean("ok")).isTrue();
        assertThat(result.getList("results.message", String.class)).containsExactly(
                "connection shop_pg created", "source shop created", "materialized view open_orders created",
                "context table open_orders created", "tableflow shop.public.orders created");
        org.mockito.Mockito.verify(loop, org.mockito.Mockito.atLeastOnce()).trigger();

        given().get("/v1/resources").then().statusCode(200)
                .body("size()", equalTo(5))
                .body("find { it.kind == 'SOURCE' }.phase", equalTo("PENDING"));
    }

    @Test
    @Order(2)
    @TestSecurity(user = "eng", roles = "engineer")
    void reApplyingIsIdempotentButChangesNeedOrReplace() {
        assertThat(sql(SOURCE).getString("results[0].message")).isEqualTo("source shop unchanged");

        JsonPath changed = sql("CREATE SOURCE shop FROM CONNECTION shop_pg TABLES (public.orders)");
        assertThat(changed.getBoolean("ok")).isFalse();
        assertThat(changed.getString("results[0].message")).contains("already exists; use CREATE OR REPLACE");

        assertThat(sql("CREATE OR REPLACE SOURCE shop FROM CONNECTION shop_pg TABLES (public.customers, public.orders, public.products)")
                .getString("results[0].message")).isEqualTo("source shop updated");
    }

    @Test
    @Order(3)
    @TestSecurity(user = "eng", roles = "engineer")
    void rejectsDanglingReferencesAndStopsAtTheFirstError() {
        JsonPath result = sql("""
                CREATE SOURCE crm FROM CONNECTION nowhere TABLES (public.accounts);
                SHOW SOURCES;""");

        assertThat(result.getBoolean("ok")).isFalse();
        assertThat(result.getList("results.status")).containsExactly("ERROR", "SKIPPED");
        assertThat(result.getString("results[0].message")).isEqualTo("connection nowhere does not exist");

        assertThat(sql("CREATE MATERIALIZED VIEW v PRIMARY KEY (id) AS SELECT 1 AS id FROM somewhere").getString("results[0].message"))
                .contains("must read at least one declared topic").contains("shop.public.orders");
        assertThat(sql("ALTER TOPIC not.a.topic ENABLE CONTEXT").getString("results[0].message"))
                .contains("does not exist and no source or materialized view produces it");
    }

    @Test
    @Order(4)
    @TestSecurity(user = "eng", roles = "engineer")
    void dropsAreBlockedByDependents() {
        JsonPath result = sql("DROP SOURCE shop");

        assertThat(result.getString("results[0].message"))
                .isEqualTo("cannot drop source shop: used by materialized view open_orders, tableflow shop.public.orders");
        assertThat(sql("DROP MATERIALIZED VIEW IF EXISTS nope").getString("results[0].message")).contains("nothing to do");
    }

    @Test
    @Order(5)
    @TestSecurity(user = "eng", roles = "engineer")
    void onlyAdminsGrant() {
        JsonPath result = sql("GRANT SELECT ON CONTEXT open_orders TO ROLE support_agent");

        assertThat(result.getString("results[0].status")).isEqualTo("ERROR");
        assertThat(result.getString("results[0].message")).startsWith("requires one of the roles [admin]");
    }

    @Test
    @Order(6)
    @TestSecurity(user = "boss", roles = "admin")
    void adminsGrantOnEnabledContextTablesOnly() {
        assertThat(sql("GRANT SELECT ON CONTEXT open_orders TO ROLE support_agent").getString("results[0].message"))
                .isEqualTo("grant SELECT ON CONTEXT open_orders TO ROLE support_agent created");
        assertThat(sql("GRANT SELECT ON CONTEXT shop.public.customers TO ROLE support_agent").getString("results[0].message"))
                .contains("context is not enabled on topic shop.public.customers");
    }

    @Test
    @Order(7)
    @TestSecurity(user = "viewer", roles = "support_agent")
    void readersCanShowAndDescribeButNotChange() {
        JsonPath show = sql("SHOW MATERIALIZED VIEWS");
        assertThat(show.getList("results[0].columns")).containsExactly("name", "status", "message");
        assertThat(show.getList("results[0].rows[0]")).first().isEqualTo("open_orders");

        JsonPath describe = sql("DESCRIBE MATERIALIZED VIEW open_orders");
        List<List<String>> rows = describe.getList("results[0].rows");
        assertThat(rows).anySatisfy(row -> assertThat(row).containsExactly("statement", VIEW));

        assertThat(sql("DROP MATERIALIZED VIEW open_orders").getString("results[0].message")).startsWith("requires one of the roles");
    }

    @Test
    @Order(8)
    @TestSecurity(user = "eng", roles = "engineer")
    void syntaxErrorsRejectTheWholeScript() {
        given().contentType(ContentType.JSON).body(Map.of("sql", "SHOW SOURCES;\nCREATE SAUCE x"))
                .post("/v1/sql").then().statusCode(400)
                .body("line", equalTo(2))
                .body("error", containsString("expected CONNECTION, SOURCE or MATERIALIZED VIEW"));
    }
}

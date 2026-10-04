package org.streamhouseoss.context;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamhouseoss.context.ingest.MaterializerManager;
import org.streamhouseoss.context.store.ServingStore;
import org.streamhouseoss.context.store.TableMode;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import jakarta.inject.Inject;

@QuarkusTest
@QuarkusTestResource(value = StackResource.class, restrictToAnnotatedClass = true)
class ContextEngineIntegrationTest {

    private static final Schema KEY = new Schema.Parser().parse("""
            {"type": "record", "name": "CustomerKey", "fields": [{"name": "id", "type": "int"}]}""");
    private static final String CUSTOMER_FIELDS = """
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string", "doc": "Full name"},
            {"name": "email", "type": ["null", "string"], "default": null},
            {"name": "vip", "type": "boolean"},
            {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "balance", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}""";
    private static final Schema CUSTOMER_V1 = new Schema.Parser().parse(
            "{\"type\": \"record\", \"name\": \"Customer\", \"fields\": [" + CUSTOMER_FIELDS + "]}");
    private static final Schema CUSTOMER_V2 = new Schema.Parser().parse(
            "{\"type\": \"record\", \"name\": \"Customer\", \"fields\": [" + CUSTOMER_FIELDS
                    + ", {\"name\": \"tier\", \"type\": [\"null\", \"string\"], \"default\": null}]}");
    private static final Schema CLICK = new Schema.Parser().parse("""
            {"type": "record", "name": "Click", "fields": [{"name": "visitor", "type": "string"}, {"name": "url", "type": "string"}]}""");

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static Fixtures fixtures;

    @Inject
    MaterializerManager materializers;

    @Inject
    ServingStore store;

    @BeforeAll
    static void setUp() {
        fixtures = new Fixtures();
    }

    @AfterAll
    static void tearDown() {
        fixtures.close();
    }

    @Test
    @TestSecurity(user = "eve", roles = "engineer")
    void upsertTombstonesTypesAndSchemaEvolution() throws Exception {
        String topic = "it.customers";
        fixtures.createTopic(topic, 3, true);
        int keyId = fixtures.register(topic + "-key", KEY);
        int v1 = fixtures.register(topic + "-value", CUSTOMER_V1);
        for (int id = 1; id <= 3; id++) {
            fixtures.send(topic, key(keyId, id), Fixtures.wire(v1, customer(CUSTOMER_V1, id, "Customer " + id, id == 1, "12.34")));
        }

        assertThat(materializers.enable(topic, null, "Customers").mode()).isEqualTo(TableMode.UPSERT);
        awaitCount(topic, 3);

        fixtures.send(topic, key(keyId, 2), Fixtures.wire(v1, customer(CUSTOMER_V1, 2, "Bob Updated", false, "0.00")));
        fixtures.send(topic, key(keyId, 3), null);
        await().atMost(WAIT).untilAsserted(() -> {
            assertThat(rows("SELECT name FROM it.customers WHERE id = 2")).containsExactly(List.of("Bob Updated"));
            assertThat(rows("SELECT COUNT(*) FROM it.customers")).containsExactly(List.of(2));
        });

        // Avro logical types arrive as proper Postgres types and ISO-8601 JSON.
        JsonPath row = query("SELECT balance, created_at, vip, email FROM \"it.customers\" WHERE id = 1").jsonPath();
        assertThat(row.getList("columns.type")).containsExactly("numeric", "timestamptz", "bool", "text");
        assertThat(row.getList("rows[0]")).containsExactly(12.34f, "2026-09-30T10:15:30Z", true, null);

        // A new optional field becomes a new nullable column.
        int v2 = fixtures.register(topic + "-value", CUSTOMER_V2);
        GenericRecord withTier = customer(CUSTOMER_V2, 4, "Dana", true, "99.99");
        withTier.put("tier", "gold");
        fixtures.send(topic, key(keyId, 4), Fixtures.wire(v2, withTier));
        await().atMost(WAIT).untilAsserted(() -> assertThat(rows("SELECT id, tier FROM it.customers WHERE tier IS NOT NULL OR id = 1 ORDER BY id"))
                .containsExactly(java.util.Arrays.asList(1, null), List.of(4, "gold")));

        JsonPath metadata = given().get("/v1/tables/it.customers").then().statusCode(200).extract().jsonPath();
        assertThat(metadata.getList("keyColumns")).containsExactly("id");
        assertThat(metadata.getString("mode")).isEqualTo("upsert");
        assertThat(metadata.getList("columns.name")).contains("tier", "_timestamp");
        assertThat(metadata.getString("columns.find { it.name == 'name' }.description")).isEqualTo("Full name");
        assertThat(metadata.getString("freshness.lastRecordTimestamp")).isNotNull();
    }

    @Test
    @TestSecurity(user = "eve", roles = "engineer")
    void appendModeIsExactlyOnceAcrossRestarts() throws Exception {
        String topic = "it.clicks";
        fixtures.createTopic(topic, 2, false);
        int clickId = fixtures.register(topic + "-value", CLICK);
        for (int i = 0; i < 100; i++) {
            fixtures.send(topic, null, Fixtures.wire(clickId, click("u" + (i % 7), "/p/" + i)));
        }
        assertThat(materializers.enable(topic, null, "Clickstream").mode()).isEqualTo(TableMode.APPEND);
        awaitCount(topic, 100);

        // Re-enabling restarts the consumer; it must resume from the offsets stored with the data.
        materializers.enable(topic, null, "Clickstream");
        for (int i = 100; i < 150; i++) {
            fixtures.send(topic, null, Fixtures.wire(clickId, click("u" + (i % 7), "/p/" + i)));
        }
        awaitCount(topic, 150);
        Thread.sleep(1500);
        assertThat(rows("SELECT COUNT(*) FROM it.clicks")).containsExactly(List.of(150));
        assertThat(rows("SELECT url FROM it.clicks WHERE visitor = 'u3' AND url LIKE '/p/14%' ORDER BY url"))
                .containsExactly(List.of("/p/143"));
    }

    @Test
    @TestSecurity(user = "support-bot", roles = "support_agent")
    void grantsControlVisibilityAndEveryQueryIsAudited() throws Exception {
        for (String topic : List.of("it.public", "it.secret")) {
            fixtures.createTopic(topic, 1, false);
            int id = fixtures.register(topic + "-value", CLICK);
            fixtures.send(topic, null, Fixtures.wire(id, click("someone", "/" + topic)));
            materializers.enable(topic, TableMode.APPEND, "");
        }

        given().get("/v1/tables").then().statusCode(200).body("name", org.hamcrest.Matchers.empty());
        given().contentType(ContentType.JSON).body(Map.of("query", "SELECT * FROM it.public"))
                .post("/v1/query").then().statusCode(404).body("error", containsString("not allowed"));

        store.grant("it.public", "support_agent");
        given().get("/v1/tables").then().statusCode(200).body("name", org.hamcrest.Matchers.contains("it.public"));
        await().atMost(WAIT).untilAsserted(() -> assertThat(rows("SELECT url FROM it.public")).containsExactly(List.of("/it.public")));
        given().contentType(ContentType.JSON).body(Map.of("query", "SELECT * FROM it.secret"))
                .post("/v1/query").then().statusCode(404);
        given().get("/v1/tables/it.secret").then().statusCode(404);

        List<String> outcomes = new ArrayList<>();
        try (Connection c = store.connection();
                ResultSet rs = c.createStatement().executeQuery(
                        "SELECT outcome FROM serving._audit WHERE principal = 'support-bot' ORDER BY id")) {
            while (rs.next()) {
                outcomes.add(rs.getString(1));
            }
        }
        assertThat(outcomes).startsWith("DENIED").contains("OK").endsWith("DENIED");
    }

    @Test
    @TestSecurity(user = "support-bot", roles = "support_agent")
    void adminApiRequiresAdminRole() {
        given().contentType(ContentType.JSON).body("{}").put("/admin/v1/tables/anything").then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "control-plane", roles = "admin")
    void enablingAMissingTopicIsAConflict() {
        given().contentType(ContentType.JSON).body("{}").put("/admin/v1/tables/no.such.topic")
                .then().statusCode(409).body("error", containsString("does not exist"));
    }

    @Test
    @TestSecurity(user = "eve", roles = "engineer")
    void invalidQueriesExplainWhatToFix() {
        given().contentType(ContentType.JSON).body(Map.of("query", "SELECT * FROM a JOIN b ON a.x = b.x"))
                .post("/v1/query").then().statusCode(400).body("error", containsString("joins"));
    }

    @Test
    @TestSecurity(user = "eve", roles = "engineer")
    void mcpToolsOverStreamableHttp() throws Exception {
        String topic = "it.mcp";
        fixtures.createTopic(topic, 1, false);
        int id = fixtures.register(topic + "-value", CLICK);
        fixtures.send(topic, null, Fixtures.wire(id, click("ada", "/hello")));
        materializers.enable(topic, TableMode.APPEND, "Clicks for the MCP test");
        awaitCount(topic, 1);

        Response init = mcp(null, Map.of("jsonrpc", "2.0", "id", 1, "method", "initialize", "params", Map.of(
                "protocolVersion", "2025-06-18", "capabilities", Map.of(),
                "clientInfo", Map.of("name", "it", "version", "1"))));
        String session = init.header("Mcp-Session-Id");
        assertThat(session).isNotBlank();
        mcp(session, Map.of("jsonrpc", "2.0", "method", "notifications/initialized"));

        JsonPath tools = rpcResult(mcp(session, Map.of("jsonrpc", "2.0", "id", 2, "method", "tools/list")));
        assertThat(tools.getList("result.tools.name")).containsExactlyInAnyOrder("listTopics", "getMetadata", "queryData");

        JsonPath listed = rpcResult(mcp(session, call(3, "listTopics", Map.of())));
        assertThat(listed.getString("result.content[0].text")).contains("Clicks for the MCP test");

        JsonPath answer = rpcResult(mcp(session, call(4, "queryData", Map.of("query", "SELECT visitor FROM it.mcp WHERE url = '/hello'"))));
        assertThat(answer.getBoolean("result.isError")).isFalse();
        assertThat(answer.getString("result.content[0].text")).contains("\"rows\":[[\"ada\"]]");

        JsonPath error = rpcResult(mcp(session, call(5, "queryData", Map.of("query", "SELECT * FROM it.mcp GROUP BY visitor"))));
        assertThat(error.getBoolean("result.isError")).isTrue();
        assertThat(error.getString("result.content[0].text")).contains("GROUP BY");
    }

    // ---- helpers -------------------------------------------------------------------------------

    private static Map<String, Object> call(int id, String tool, Map<String, Object> args) {
        return Map.of("jsonrpc", "2.0", "id", id, "method", "tools/call", "params", Map.of("name", tool, "arguments", args));
    }

    private static Response mcp(String session, Map<String, Object> message) {
        var request = given().contentType(ContentType.JSON).accept("application/json, text/event-stream").body(message);
        if (session != null) {
            request.header("Mcp-Session-Id", session);
        }
        Response response = request.post("/mcp");
        assertThat(response.statusCode()).as(response.asString()).isBetween(200, 202);
        return response;
    }

    /** JSON-RPC responses come back as JSON or as a single SSE event, depending on the server. */
    private static JsonPath rpcResult(Response response) {
        String body = response.asString();
        if (body.startsWith("event:") || body.startsWith("data:") || body.contains("\ndata:")) {
            body = body.lines().filter(l -> l.startsWith("data:")).map(l -> l.substring(5).trim()).reduce("", String::concat);
        }
        return new JsonPath(body);
    }

    private static void awaitCount(String topic, int expected) {
        await().atMost(WAIT).untilAsserted(() -> assertThat(rows("SELECT COUNT(*) FROM \"" + topic + "\""))
                .containsExactly(List.of(expected)));
    }

    private static Response query(String sql) {
        Response response = given().contentType(ContentType.JSON).body(Map.of("query", sql)).post("/v1/query");
        assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
        return response;
    }

    private static List<List<Object>> rows(String sql) {
        return query(sql).jsonPath().getList("rows");
    }

    private static byte[] key(int schemaId, int id) {
        GenericRecord key = new GenericData.Record(KEY);
        key.put("id", id);
        return Fixtures.wire(schemaId, key);
    }

    private static GenericRecord customer(Schema schema, int id, String name, boolean vip, String balance) {
        GenericRecord r = new GenericData.Record(schema);
        r.put("id", id);
        r.put("name", name);
        r.put("vip", vip);
        r.put("created_at", Instant.parse("2026-09-30T10:15:30Z").toEpochMilli());
        Schema balanceSchema = schema.getField("balance").schema();
        ByteBuffer bytes = new Conversions.DecimalConversion().toBytes(new BigDecimal(balance), balanceSchema,
                LogicalTypes.decimal(10, 2));
        r.put("balance", bytes);
        return r;
    }

    private static GenericRecord click(String visitor, String url) {
        GenericRecord r = new GenericData.Record(CLICK);
        r.put("visitor", visitor);
        r.put("url", url);
        return r;
    }
}

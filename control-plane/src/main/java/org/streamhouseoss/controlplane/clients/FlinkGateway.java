package org.streamhouseoss.controlplane.clients;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Runs SQL through the Flink SQL Gateway. Catalogs, temporary tables and SET options are scoped to
 * a session, so each submission opens a session, declares what it needs, submits and closes.
 * INSERT jobs keep running after the session is closed.
 */
@ApplicationScoped
public class FlinkGateway {

    private static final Duration STATEMENT_TIMEOUT = Duration.ofSeconds(60);

    /** Rows returned by a statement, plus the job id when it submitted one. */
    public record Result(List<String> columns, List<List<JsonNode>> rows, String jobId) {
    }

    /** A Flink column: name, SQL type and nullability. */
    public record Column(String name, String type, boolean nullable) {
    }

    /** Physical columns and primary key of a table. */
    public record Described(List<Column> columns, List<String> primaryKey) {
    }

    private final FlinkGatewayApi api;

    public FlinkGateway(@RestClient FlinkGatewayApi api) {
        this.api = api;
    }

    public Session open(String name) {
        String handle = Http.ok(api.openSession(Map.of("sessionName", name)), "opening a Flink SQL session")
                .getEntity().path("sessionHandle").asText();
        return new Session(handle);
    }

    public final class Session implements AutoCloseable {
        private final String handle;

        private Session(String handle) {
            this.handle = handle;
        }

        /** Executes one statement and waits for its complete result. */
        public Result execute(String statement) {
            RestResponse<JsonNode> submitted = api.execute(handle, Map.of("statement", statement));
            if (submitted.getStatus() >= 300) {
                throw new ComponentException(flinkError(submitted.getEntity(), statement));
            }
            String operation = submitted.getEntity().path("operationHandle").asText();
            List<String> columns = new ArrayList<>();
            List<List<JsonNode>> rows = new ArrayList<>();
            String jobId = null;
            Instant deadline = Instant.now().plus(STATEMENT_TIMEOUT);
            Long token = 0L;
            while (token != null) {
                RestResponse<JsonNode> page = api.result(handle, operation, token, "JSON");
                if (page.getStatus() >= 300) {
                    throw new ComponentException(flinkError(page.getEntity(), statement));
                }
                JsonNode body = page.getEntity();
                if (body.hasNonNull("jobID")) {
                    jobId = body.get("jobID").asText();
                }
                String type = body.path("resultType").asText();
                if ("NOT_READY".equals(type)) {
                    if (Instant.now().isAfter(deadline)) {
                        throw new ComponentException("Flink statement timed out: " + abbreviate(statement));
                    }
                    Poll.sleep(Duration.ofMillis(200));
                    continue;
                }
                JsonNode results = body.path("results");
                if (columns.isEmpty()) {
                    results.path("columns").forEach(c -> columns.add(c.path("name").asText()));
                }
                for (JsonNode row : results.path("data")) {
                    List<JsonNode> fields = new ArrayList<>();
                    row.path("fields").forEach(fields::add);
                    rows.add(fields);
                }
                token = "EOS".equals(type) || !body.hasNonNull("nextResultUri") ? null : nextToken(body.get("nextResultUri").asText());
            }
            api.closeOperation(handle, operation);
            return new Result(columns, rows, jobId);
        }

        /** Physical columns and primary key of a table, from DESCRIBE (metadata and computed columns skipped). */
        public Described describe(String table) {
            Result result = execute("DESCRIBE " + table);
            int name = result.columns().indexOf("name");
            int type = result.columns().indexOf("type");
            int nullable = result.columns().indexOf("null");
            int key = result.columns().indexOf("key");
            int extras = result.columns().indexOf("extras");
            List<Column> columns = new ArrayList<>();
            List<String> primaryKey = new ArrayList<>();
            for (List<JsonNode> row : result.rows()) {
                String column = row.get(name).asText();
                String extra = extras < 0 || row.get(extras).isNull() ? "" : row.get(extras).asText();
                String columnType = row.get(type).asText();
                if (column.startsWith("$") || extra.contains("METADATA") || extra.startsWith("AS ") || columnType.contains("*ROWTIME*")) {
                    continue;
                }
                columns.add(new Column(column, columnType.replaceAll(" NOT NULL$", ""), row.get(nullable).asBoolean(true)));
                if (key >= 0 && row.get(key).asText("").startsWith("PRI")) {
                    primaryKey.add(column);
                }
            }
            return new Described(columns, primaryKey);
        }

        @Override
        public void close() {
            api.closeSession(handle);
        }
    }

    /** {@code /v3/sessions/S/operations/O/result/7?rowFormat=JSON} → 7 */
    static long nextToken(String nextResultUri) {
        String path = nextResultUri.split("\\?")[0];
        return Long.parseLong(path.substring(path.lastIndexOf('/') + 1));
    }

    /** Flink's errors list ends with the root cause; keep the most useful line. */
    private static String flinkError(JsonNode body, String statement) {
        List<String> errors = new ArrayList<>();
        if (body != null) {
            body.path("errors").forEach(e -> errors.add(e.asText()));
        }
        String detail = errors.isEmpty() ? String.valueOf(body) : rootCause(String.join("\n", errors));
        return "Flink rejected `" + abbreviate(statement) + "`: " + detail;
    }

    static String rootCause(String trace) {
        String cause = null;
        for (String line : trace.split("\n")) {
            String l = line.strip();
            if (l.startsWith("Caused by: ")) {
                cause = l.substring("Caused by: ".length());
            }
        }
        if (cause == null) {
            cause = trace.lines().filter(l -> !l.isBlank() && !l.startsWith("\tat ") && !l.startsWith("at "))
                    .reduce((a, b) -> b).orElse(trace);
        }
        return Http.abbreviate(cause.replaceFirst("^[\\w.$]+(Exception|Error): ", ""));
    }

    private static String abbreviate(String statement) {
        String oneLine = statement.replaceAll("\\s+", " ").strip();
        return oneLine.length() > 120 ? oneLine.substring(0, 120) + "…" : oneLine;
    }
}

package org.streamhouseoss.controlplane.clients;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.streamhouseoss.controlplane.StreamhouseConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Flink SQL Gateway REST API (v3). Catalogs, temporary tables and SET options are scoped to a
 * session, so each job submission opens a session, declares what it needs, submits and closes.
 * INSERT jobs keep running after the session is closed.
 */
@ApplicationScoped
public class FlinkGateway {

    private static final Duration STATEMENT_TIMEOUT = Duration.ofSeconds(60);

    private final JsonHttp http;

    public FlinkGateway(StreamhouseConfig config, ObjectMapper mapper) {
        this.http = new JsonHttp(mapper, config.flink().gatewayUrl(), Map.of());
    }

    /** Rows returned by a statement, plus the job id when it submitted one. */
    public record Result(List<String> columns, List<List<JsonNode>> rows, String jobId) {
    }

    public Session open(String name) {
        String handle = http.post("/v3/sessions", Map.of("sessionName", name))
                .requireOk("opening Flink SQL session").body().path("sessionHandle").asText();
        return new Session(handle);
    }

    public final class Session implements AutoCloseable {
        private final String handle;

        private Session(String handle) {
            this.handle = handle;
        }

        /** Executes one statement and waits for its complete result. */
        public Result execute(String statement) {
            JsonHttp.Response submitted = http.post("/v3/sessions/" + handle + "/statements", Map.of("statement", statement));
            if (!submitted.ok()) {
                throw new ComponentException(flinkError(submitted, statement));
            }
            String operation = submitted.body().path("operationHandle").asText();
            String next = "/v3/sessions/" + handle + "/operations/" + operation + "/result/0?rowFormat=JSON";
            List<String> columns = new ArrayList<>();
            List<List<JsonNode>> rows = new ArrayList<>();
            String jobId = null;
            Instant deadline = Instant.now().plus(STATEMENT_TIMEOUT);
            while (next != null) {
                JsonHttp.Response page = http.get(next);
                if (!page.ok()) {
                    throw new ComponentException(flinkError(page, statement));
                }
                JsonNode body = page.body();
                if (body.hasNonNull("jobID")) {
                    jobId = body.get("jobID").asText();
                }
                String type = body.path("resultType").asText();
                if ("NOT_READY".equals(type)) {
                    if (Instant.now().isAfter(deadline)) {
                        throw new ComponentException("Flink statement timed out: " + abbreviate(statement));
                    }
                    sleep(200);
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
                next = "EOS".equals(type) || !body.hasNonNull("nextResultUri") ? null : body.get("nextResultUri").asText();
            }
            http.delete("/v3/sessions/" + handle + "/operations/" + operation + "/close");
            return new Result(columns, rows, jobId);
        }

        @Override
        public void close() {
            http.delete("/v3/sessions/" + handle);
        }
    }

    /** Flink's errors list ends with the root cause; keep the most useful line. */
    private static String flinkError(JsonHttp.Response response, String statement) {
        List<String> errors = new ArrayList<>();
        response.body().path("errors").forEach(e -> errors.add(e.asText()));
        String detail = errors.isEmpty() ? response.raw() : rootCause(String.join("\n", errors));
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
        return JsonHttp.abbreviate(cause.replaceFirst("^[\\w.$]+(Exception|Error): ", ""));
    }

    private static String abbreviate(String statement) {
        String oneLine = statement.replaceAll("\\s+", " ").strip();
        return oneLine.length() > 120 ? oneLine.substring(0, 120) + "…" : oneLine;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ComponentException("interrupted", e);
        }
    }
}

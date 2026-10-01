package org.streamhouseoss.cli;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "query", description = "Run a lightning query against the context engine, e.g. "
        + "shctl query \"SELECT * FROM customer_360 WHERE customer_id = 42\"")
class QueryCommand extends BaseCommand {

    @Parameters(index = "0", paramLabel = "SQL")
    String sql;

    @Override
    int run() {
        JsonNode result = session().post(endpoints.contextEngine, "/v1/query", Map.of("query", sql));
        Table.print(System.out, result.path("columns"), result.path("rows"));
        JsonNode freshness = result.path("freshness");
        System.out.printf("%n%d row(s) in %d ms; latest record %s%s%n", result.path("rowCount").asInt(),
                result.path("elapsedMs").asLong(), freshness.path("lastRecordTimestamp").asText("n/a"),
                result.path("note").isTextual() ? "\n" + result.path("note").asText() : "");
        return 0;
    }
}

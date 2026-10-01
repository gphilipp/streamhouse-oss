package org.streamhouseoss.cli;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "describe", description = "Show a resource's status, definition and lineage.")
class DescribeCommand extends BaseCommand {

    @Parameters(index = "0", paramLabel = "KIND", description = "connection, source, view, tableflow or context")
    String kind;

    @Parameters(index = "1", paramLabel = "NAME")
    String name;

    @Override
    int run() {
        String sqlKind = switch (kind.toLowerCase().replace('-', '_')) {
            case "view", "mv", "materialized_view" -> "MATERIALIZED VIEW";
            case "context", "context_table" -> "CONTEXT";
            default -> kind.toUpperCase();
        };
        JsonNode result = session().post(endpoints.server, "/v1/sql", Map.of("sql", "DESCRIBE " + sqlKind + " " + quote(name)));
        JsonNode statement = result.path("results").get(0);
        if (!statement.path("status").asText().equals("OK")) {
            throw new CliException(statement.path("message").asText());
        }
        for (JsonNode row : statement.path("rows")) {
            String property = row.get(0).asText();
            String value = row.get(1).asText();
            if (property.equals("upstream") || property.equals("downstream")) {
                System.out.println(property + ":");
                if (value.isBlank()) {
                    System.out.println("  (none)");
                }
                for (String edge : value.split("; ")) {
                    if (!edge.isBlank()) {
                        System.out.println("  " + edge);
                    }
                }
            } else {
                System.out.printf("%-12s %s%n", property + ":", value.replace("\n", "\n             "));
            }
        }
        return 0;
    }

    private static String quote(String name) {
        return name.matches("[a-z_][a-z0-9_.]*") ? name : "\"" + name.replace("\"", "\"\"") + "\"";
    }
}

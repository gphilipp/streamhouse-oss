package org.streamhouseoss.cli;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "get", description = "List resources and their status.")
class GetCommand extends BaseCommand {

    @Parameters(arity = "0..1", paramLabel = "KIND",
            description = "connection, source, materialized_view, iceberg_table, context_table or grant (default: all)")
    String kind;

    @Override
    int run() {
        JsonNode resources = session().resources(kind == null ? null : kind.replace('-', '_').replace(' ', '_'));
        List<List<String>> rows = new ArrayList<>();
        for (JsonNode r : resources) {
            rows.add(List.of(r.path("kind").asText().toLowerCase(), r.path("name").asText(), r.path("phase").asText(),
                    r.path("message").asText("")));
        }
        Table.print(System.out, List.of("KIND", "NAME", "STATUS", "MESSAGE"), rows);
        return 0;
    }
}

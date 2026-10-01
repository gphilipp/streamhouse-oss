package org.streamhouseoss.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sql", description = "Run streamhouse SQL (CREATE SOURCE, CREATE MATERIALIZED VIEW, ALTER TOPIC ..., SHOW, DESCRIBE).")
class SqlCommand extends BaseCommand {

    static class Input {
        @Option(names = { "-f", "--file" }, description = "Script file ('-' for stdin)")
        Path file;

        @Option(names = { "-e", "--execute" }, description = "Statements to run")
        String sql;
    }

    @ArgGroup(multiplicity = "1")
    Input input;

    @Option(names = "--wait", description = "Wait until every resource has been reconciled; fail if any ends up FAILED")
    boolean waitReady;

    @Option(names = "--timeout", defaultValue = "300", description = "Seconds to wait with --wait (default ${DEFAULT-VALUE})")
    long timeoutSeconds;

    @Override
    int run() {
        String sql = readInput();
        Session session = session();
        JsonNode result;
        try {
            result = session.post(endpoints.server, "/v1/sql", Map.of("sql", sql));
        } catch (CliException e) {
            if (e.body() != null && e.body().has("line")) {
                throw new CliException("syntax error at " + e.getMessage());
            }
            throw e;
        }
        boolean ok = result.path("ok").asBoolean();
        for (JsonNode statement : result.path("results")) {
            String status = statement.path("status").asText();
            if (statement.path("columns").isEmpty()) {
                System.out.println(switch (status) {
                    case "OK" -> "✓ " + statement.path("message").asText();
                    case "SKIPPED" -> "- " + statement.path("message").asText();
                    default -> "✗ " + firstLine(statement.path("statement").asText()) + "\n  " + statement.path("message").asText();
                });
            } else {
                Table.print(System.out, statement.path("columns"), statement.path("rows"));
                System.out.println();
            }
        }
        if (!ok) {
            return 1;
        }
        return waitReady ? waitUntilSettled(session) : 0;
    }

    private int waitUntilSettled(Session session) {
        Instant deadline = Instant.now().plusSeconds(timeoutSeconds);
        String last = "";
        while (true) {
            JsonNode resources = session.get(endpoints.server, "/v1/resources");
            List<JsonNode> pending = new ArrayList<>();
            List<JsonNode> failed = new ArrayList<>();
            for (JsonNode r : resources) {
                if (!r.path("settled").asBoolean() || r.path("phase").asText().equals("PENDING") || r.path("phase").asText().equals("DELETING")) {
                    pending.add(r);
                } else if (r.path("phase").asText().equals("FAILED")) {
                    failed.add(r);
                }
            }
            if (pending.isEmpty() || Instant.now().isAfter(deadline)) {
                for (JsonNode r : failed) {
                    System.out.println("✗ " + describe(r));
                }
                for (JsonNode r : pending) {
                    System.out.println("… " + describe(r));
                }
                if (!pending.isEmpty()) {
                    System.out.println("timed out after " + timeoutSeconds + "s");
                    return 1;
                }
                System.out.println(failed.isEmpty() ? "All " + resources.size() + " resources are ready." : failed.size() + " resource(s) failed.");
                return failed.isEmpty() ? 0 : 1;
            }
            String progress = "waiting for " + pending.size() + " resource(s): " + describe(pending.getFirst());
            if (!progress.equals(last)) {
                System.out.println(progress);
                last = progress;
            }
            try {
                Thread.sleep(Duration.ofSeconds(2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return 1;
            }
        }
    }

    private static String describe(JsonNode r) {
        return r.path("kind").asText().toLowerCase().replace('_', ' ') + " " + r.path("name").asText() + ": "
                + r.path("phase").asText() + " - " + r.path("message").asText("");
    }

    private String readInput() {
        if (input.sql != null) {
            return input.sql;
        }
        try {
            return input.file.toString().equals("-") ? new String(System.in.readAllBytes()) : Files.readString(input.file);
        } catch (IOException e) {
            throw new CliException("cannot read " + input.file + ": " + e.getMessage());
        }
    }

    private static String firstLine(String s) {
        return s.lines().findFirst().orElse(s);
    }
}

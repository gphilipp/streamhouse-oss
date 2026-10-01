package org.streamhouseoss.cli;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

/** Prints rows as an aligned text table, truncating long cells. */
final class Table {

    private static final int MAX_WIDTH = 80;

    private Table() {
    }

    static void print(PrintStream out, List<String> columns, List<List<String>> rows) {
        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            widths[i] = columns.get(i).length();
            for (List<String> row : rows) {
                widths[i] = Math.min(MAX_WIDTH, Math.max(widths[i], row.get(i).length()));
            }
        }
        out.println(line(columns, widths));
        StringBuilder rule = new StringBuilder();
        for (int i = 0; i < widths.length; i++) {
            rule.append(i == 0 ? "" : "  ").append("-".repeat(widths[i]));
        }
        out.println(rule);
        rows.forEach(r -> out.println(line(r, widths)));
    }

    /** Prints a result with {@code columns} (strings or {name}) and {@code rows} arrays. */
    static void print(PrintStream out, JsonNode columns, JsonNode rows) {
        List<String> names = new ArrayList<>();
        columns.forEach(c -> names.add(c.isTextual() ? c.asText() : c.path("name").asText()));
        List<List<String>> cells = new ArrayList<>();
        for (JsonNode row : rows) {
            List<String> cellRow = new ArrayList<>();
            row.forEach(v -> cellRow.add(v.isNull() ? "NULL" : v.isValueNode() ? v.asText() : v.toString()));
            cells.add(cellRow);
        }
        print(out, names, cells);
    }

    private static String line(List<String> cells, int[] widths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cells.size(); i++) {
            String cell = cells.get(i).replace('\n', ' ');
            if (cell.length() > widths[i]) {
                cell = cell.substring(0, widths[i] - 1) + "…";
            }
            sb.append(i == 0 ? "" : "  ").append(i == cells.size() - 1 ? cell : String.format("%-" + widths[i] + "s", cell));
        }
        return sb.toString();
    }
}

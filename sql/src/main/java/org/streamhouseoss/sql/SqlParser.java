package org.streamhouseoss.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.streamhouseoss.model.ConnectionType;
import org.streamhouseoss.model.OptionValue;
import org.streamhouseoss.model.Privilege;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.model.TableMode;
import org.streamhouseoss.model.TableRef;
import org.streamhouseoss.sql.parser.SqlStreamhouseStatement;
import org.streamhouseoss.sql.parser.SqlStreamhouseStatement.Option;
import org.streamhouseoss.sql.parser.impl.StreamhouseSqlParserImpl;

/**
 * Parses streamhouse SQL with Apache Calcite's parser, extended with the streamhouse DDL (see
 * {@code src/main/codegen}). Identifiers are quoted with back ticks, like Flink SQL; unquoted ones
 * are case-insensitive and normalized to lower case.
 */
public final class SqlParser {

    private static final org.apache.calcite.sql.parser.SqlParser.Config CONFIG = org.apache.calcite.sql.parser.SqlParser.config()
            .withParserFactory(StreamhouseSqlParserImpl.FACTORY)
            .withQuoting(Quoting.BACK_TICK)
            .withUnquotedCasing(Casing.TO_LOWER)
            .withQuotedCasing(Casing.UNCHANGED)
            .withIdentifierMaxLength(200);

    private final String input;
    private final int[] lineStarts;

    private SqlParser(String input) {
        this.input = input;
        List<Integer> starts = new ArrayList<>(List.of(0));
        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == '\n') {
                starts.add(i + 1);
            }
        }
        this.lineStarts = starts.stream().mapToInt(Integer::intValue).toArray();
    }

    /** Parses a script of {@code ;}-separated statements. */
    public static List<Statement> parseScript(String script) {
        return new SqlParser(script).script();
    }

    /** Parses exactly one statement (a trailing {@code ;} is allowed). */
    public static Statement parseStatement(String sql) {
        List<Statement> statements = parseScript(sql);
        if (statements.size() != 1) {
            throw new SqlParseException("expected exactly one statement, got " + statements.size(), 1);
        }
        return statements.getFirst();
    }

    private List<Statement> script() {
        List<SqlNode> nodes;
        try {
            nodes = org.apache.calcite.sql.parser.SqlParser.create(input, CONFIG).parseStmtList().getList();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            throw syntaxError(e);
        }
        List<Statement> statements = new ArrayList<>();
        for (SqlNode node : nodes) {
            if (!(node instanceof SqlStreamhouseStatement st)) {
                throw new SqlParseException("expected CREATE, DROP, ALTER TOPIC, GRANT, REVOKE, SHOW or DESCRIBE",
                        node.getParserPosition().getLineNum());
            }
            statements.add(toStatement(st, slice(st.getParserPosition())));
        }
        return statements;
    }

    private Statement toStatement(SqlStreamhouseStatement st, String text) {
        int line = st.getParserPosition().getLineNum();
        return switch (st.verb) {
            case CREATE_CONNECTION -> {
                String typeName = name(st.target);
                ConnectionType type = build(line, () -> ConnectionType.valueOf(typeName.toUpperCase(Locale.ROOT)),
                        "unsupported connection type " + typeName);
                Map<String, OptionValue> options = connectionOptions(st.options);
                yield apply(line, () -> new Resource.Connection(name(st.name), type, options), st.replace, text);
            }
            case CREATE_SOURCE -> {
                List<TableRef> tables = new ArrayList<>();
                for (SqlIdentifier table : st.identifiers) {
                    if (table.names.size() != 2) {
                        throw new SqlParseException("expected schema.table, got " + String.join(".", table.names),
                                table.getParserPosition().getLineNum());
                    }
                    tables.add(build(table.getParserPosition().getLineNum(),
                            () -> new TableRef(table.names.get(0), table.names.get(1)), null));
                }
                yield apply(line, () -> new Resource.Source(name(st.name), name(st.target), tables), st.replace, text);
            }
            case CREATE_MATERIALIZED_VIEW -> apply(line, () -> new Resource.MaterializedView(name(st.name),
                    st.identifiers.stream().map(SqlParser::name).toList(), slice(st.raw)), st.replace, text);
            case CREATE_STATEMENT -> apply(line, () -> new Resource.Statement(name(st.name), slice(st.raw)), st.replace, text);
            case ENABLE -> {
                String topic = topic(st.name);
                if (st.object.equals("ICEBERG")) {
                    Map<String, String> options = literalOptions(st.options, Set.of("mode"));
                    TableMode mode = mode(options.get("mode"), line);
                    yield apply(line, () -> new Resource.IcebergTable(topic, mode), true, text);
                }
                Map<String, String> options = literalOptions(st.options, Set.of("mode", "description"));
                TableMode mode = mode(options.get("mode"), line);
                yield apply(line, () -> new Resource.ContextTable(topic, mode, options.get("description")), true, text);
            }
            case DISABLE -> {
                noOptions(st);
                yield new Statement.Remove(st.object.equals("ICEBERG") ? ResourceKind.ICEBERG_TABLE : ResourceKind.CONTEXT_TABLE,
                        topic(st.name), true, text);
            }
            case GRANT -> apply(line, () -> grant(st), true, text);
            case REVOKE -> new Statement.Remove(ResourceKind.GRANT, build(line, () -> grant(st), null).name(), true, text);
            case DROP -> new Statement.Remove(kind(st.object), name(st.name), st.ifExists, text);
            case SHOW -> new Statement.Show(st.object.equals("TOPICS") ? null : kind(st.object), text);
            case DESCRIBE -> {
                ResourceKind kind = kind(st.object);
                boolean topicName = kind == ResourceKind.ICEBERG_TABLE || kind == ResourceKind.CONTEXT_TABLE;
                yield new Statement.Describe(kind, topicName ? topic(st.name) : name(st.name), text);
            }
        };
    }

    private static Resource.Grant grant(SqlStreamhouseStatement st) {
        return new Resource.Grant(Privilege.SELECT, ResourceKind.CONTEXT_TABLE, topic(st.name), name(st.target));
    }

    private static ResourceKind kind(String object) {
        return switch (object) {
            case "CONNECTION" -> ResourceKind.CONNECTION;
            case "SOURCE" -> ResourceKind.SOURCE;
            case "MATERIALIZED VIEW" -> ResourceKind.MATERIALIZED_VIEW;
            case "STATEMENT" -> ResourceKind.STATEMENT;
            case "ICEBERG" -> ResourceKind.ICEBERG_TABLE;
            case "CONTEXT" -> ResourceKind.CONTEXT_TABLE;
            case "GRANT" -> ResourceKind.GRANT;
            default -> throw new IllegalStateException("unknown object kind " + object);
        };
    }

    private static TableMode mode(String value, int line) {
        if (value == null) {
            return null;
        }
        return build(line, () -> TableMode.valueOf(value.toUpperCase(Locale.ROOT)),
                "mode must be 'append' or 'upsert', got '" + value + "'");
    }

    private static Map<String, OptionValue> connectionOptions(List<Option> options) {
        Map<String, OptionValue> result = new LinkedHashMap<>();
        for (Option option : options) {
            OptionValue value = option.secret() ? new OptionValue.Secret(literal(option)) : new OptionValue.Literal(literal(option));
            if (result.put(option.key(), value) != null) {
                throw new SqlParseException("duplicate option " + option.key(), option.pos().getLineNum());
            }
        }
        return result;
    }

    /** Options restricted to the given keys and to literal values. */
    private static Map<String, String> literalOptions(List<Option> options, Set<String> allowed) {
        Map<String, String> result = new LinkedHashMap<>();
        for (Option option : options) {
            int line = option.pos().getLineNum();
            if (!allowed.contains(option.key())) {
                throw new SqlParseException("unknown option " + option.key() + "; allowed: "
                        + String.join(", ", allowed.stream().sorted().toList()), line);
            }
            if (option.secret()) {
                throw new SqlParseException("option " + option.key() + " cannot be a secret", line);
            }
            if (result.put(option.key(), literal(option)) != null) {
                throw new SqlParseException("duplicate option " + option.key(), line);
            }
        }
        return result;
    }

    private static void noOptions(SqlStreamhouseStatement st) {
        if (!st.options.isEmpty()) {
            throw new SqlParseException("DISABLE takes no options", st.options.getFirst().pos().getLineNum());
        }
    }

    private static String literal(Option option) {
        if (!(option.value() instanceof SqlLiteral literal) || literal.getValue() == null) {
            throw new SqlParseException("option " + option.key() + " needs a string, number or boolean",
                    option.pos().getLineNum());
        }
        return switch (literal) {
            case SqlCharStringLiteral string -> string.getValueAs(String.class);
            case SqlLiteral bool when bool.getTypeName() == SqlTypeName.BOOLEAN -> String.valueOf(bool.booleanValue());
            default -> literal.toValue();
        };
    }

    private static String name(SqlIdentifier id) {
        if (!id.isSimple()) {
            throw new SqlParseException("expected a simple name, got " + String.join(".", id.names),
                    id.getParserPosition().getLineNum());
        }
        return id.getSimple();
    }

    /** Topic names may be written unquoted with dots (shop.public.orders) or quoted (`shop.public.orders`). */
    private static String topic(SqlIdentifier id) {
        return String.join(".", id.names);
    }

    private static Statement apply(int line, Supplier<Resource> resource, boolean orReplace, String text) {
        return new Statement.Apply(build(line, resource, null), orReplace, text);
    }

    /** Runs a model constructor, reporting its validation errors at the statement's line. */
    private static <T> T build(int line, Supplier<T> constructor, String message) {
        try {
            return constructor.get();
        } catch (IllegalArgumentException e) {
            throw new SqlParseException(message != null ? message : e.getMessage(), line);
        }
    }

    /** The original text between two positions (lines and columns are 1-based, end inclusive). */
    private String slice(SqlParserPos pos) {
        int start = lineStarts[pos.getLineNum() - 1] + pos.getColumnNum() - 1;
        int end = lineStarts[pos.getEndLineNum() - 1] + pos.getEndColumnNum();
        return input.substring(start, Math.min(end, input.length()));
    }

    private static SqlParseException syntaxError(org.apache.calcite.sql.parser.SqlParseException e) {
        SqlParserPos pos = e.getPos();
        String first = e.getMessage().lines().findFirst().orElse(e.getMessage());
        String found = first.startsWith("Encountered ") && first.contains("\" at line")
                ? first.substring("Encountered ".length(), first.indexOf(" at line"))
                : null;
        // Calcite lists every identifier token kind (<IDENTIFIER>, <BACK_QUOTED_IDENTIFIER>, ...): say "a name".
        List<String> expected = e.getExpectedTokenNames().stream()
                .map(t -> t.contains("IDENTIFIER") ? "a name" : t.replace("\"", ""))
                .distinct().sorted().toList();
        String message;
        if (found != null && !expected.isEmpty() && expected.size() <= 12) {
            message = "syntax error at " + found + "; expected " + String.join(", ", expected);
        } else if (found != null) {
            message = "syntax error at " + found;
        } else {
            message = first.replaceFirst("^org\\.apache\\.calcite\\.[\\w.]+: ", "");
        }
        return new SqlParseException(message, pos == null ? 1 : pos.getLineNum());
    }
}

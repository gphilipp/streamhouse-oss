package org.streamhouseoss.context.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.streamhouseoss.context.schema.Column;
import org.streamhouseoss.context.schema.ColumnType;
import org.streamhouseoss.context.store.ServingStore;
import org.streamhouseoss.context.store.TableInfo;

/**
 * Compiles lightning queries: a deliberately small, single-table subset of SQL that is safe to
 * expose to apps and AI agents.
 *
 * <pre>
 * SELECT * | col [AS alias], ... | COUNT(*) | SUM/AVG/MIN/MAX(col), ...
 * FROM topic [AS alias]
 * [WHERE predicates combined with AND / OR / NOT:
 *        col (= | &lt;&gt; | &lt; | &lt;= | &gt; | &gt;=) literal, col [NOT] BETWEEN a AND b,
 *        col [NOT] IN (literals), col [NOT] LIKE 'pattern', col IS [NOT] NULL / TRUE / FALSE]
 * [ORDER BY col [ASC | DESC], ...]
 * [LIMIT n] [OFFSET m]
 * </pre>
 *
 * The query is re-rendered against the serving table with every literal bound as a parameter
 * and cast to the column's type, so user input never becomes SQL text.
 */
public final class LightningQueryCompiler {

    /** A query whose table is known but whose columns are not yet validated. */
    public record Parsed(String table, SqlSelect select, SqlNodeList orderBy, SqlNode offset, SqlNode fetch, String alias) {
    }

    public record Compiled(String table, String sql, List<String> parameters, int limit) {
    }

    private static final SqlParser.Config PARSER = SqlParser.config()
            .withQuoting(Quoting.DOUBLE_QUOTE)
            .withQuotedCasing(Casing.UNCHANGED)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withCaseSensitive(true);

    private static final int MAX_OFFSET = 100_000;
    private static final Set<String> AGGREGATES = Set.of("COUNT", "SUM", "AVG", "MIN", "MAX");

    private final int maxRows;
    private final int defaultRows;

    public LightningQueryCompiler(int maxRows, int defaultRows) {
        this.maxRows = maxRows;
        this.defaultRows = defaultRows;
    }

    /**
     * Parses a query and resolves its table name against the known tables (exact match first,
     * then case-insensitive).
     */
    public Parsed parse(String sql, Collection<String> knownTables) {
        SqlNode node;
        try {
            node = SqlParser.create(stripTrailingSemicolon(sql), PARSER).parseQuery();
        } catch (SqlParseException e) {
            throw QueryException.invalid("syntax error: " + firstLine(e.getMessage()));
        }
        SqlNodeList orderBy = null;
        SqlNode offset = null;
        SqlNode fetch = null;
        if (node instanceof SqlOrderBy ob) {
            orderBy = ob.orderList;
            offset = ob.offset;
            fetch = ob.fetch;
            node = ob.query;
        }
        if (!(node instanceof SqlSelect select)) {
            throw QueryException.invalid("only single-table SELECT statements are supported");
        }
        if (orderBy == null) {
            orderBy = select.getOrderList();
            offset = select.getOffset();
            fetch = select.getFetch();
        }
        if (select.getGroup() != null || select.getHaving() != null || !select.getWindowList().isEmpty()
                || select.getQualify() != null) {
            throw QueryException.invalid("GROUP BY, HAVING, WINDOW and QUALIFY are not supported");
        }
        if (select.isDistinct()) {
            throw QueryException.invalid("SELECT DISTINCT is not supported");
        }

        SqlNode from = select.getFrom();
        String alias = null;
        if (from instanceof SqlBasicCall as && as.getKind() == SqlKind.AS) {
            alias = ((SqlIdentifier) as.operand(1)).getSimple();
            from = as.operand(0);
        }
        if (!(from instanceof SqlIdentifier tableId) || tableId.isStar()) {
            throw QueryException.invalid("query exactly one table: joins, subqueries and table functions are not supported");
        }
        String requested = String.join(".", tableId.names);
        String table = resolve(requested, knownTables)
                .orElseThrow(() -> new QueryException(QueryException.Reason.NOT_FOUND,
                        "table " + requested + " does not exist or you are not allowed to query it"));
        return new Parsed(table, select, orderBy, offset, fetch, alias);
    }

    /** Validates columns against the table and renders the parameterized Postgres query. */
    public Compiled compile(Parsed parsed, TableInfo table) {
        return new Renderer(parsed, table).render();
    }

    private final class Renderer {
        private final Parsed parsed;
        private final TableInfo table;
        private final List<String> parameters = new ArrayList<>();
        private final List<String> outputAliases = new ArrayList<>();

        Renderer(Parsed parsed, TableInfo table) {
            this.parsed = parsed;
            this.table = table;
        }

        Compiled render() {
            StringBuilder sql = new StringBuilder("SELECT ");
            boolean aggregate = renderSelectList(sql);
            sql.append(" FROM ").append(ServingStore.qualified(table.topic()));
            if (parsed.select().getWhere() != null) {
                sql.append(" WHERE ").append(condition(parsed.select().getWhere()));
            }
            if (parsed.orderBy() != null && !parsed.orderBy().isEmpty()) {
                if (aggregate) {
                    throw QueryException.invalid("ORDER BY cannot be combined with aggregate functions");
                }
                sql.append(" ORDER BY ").append(parsed.orderBy().stream().map(this::orderItem).collect(Collectors.joining(", ")));
            }
            int limit = aggregate ? 1 : limit();
            if (!aggregate) {
                sql.append(" LIMIT ").append(limit + 1); // one extra row tells whether results were truncated
            }
            if (parsed.offset() != null) {
                int offset = integer(parsed.offset(), "OFFSET");
                if (offset > MAX_OFFSET) {
                    throw QueryException.invalid("OFFSET must be at most " + MAX_OFFSET);
                }
                sql.append(" OFFSET ").append(offset);
            }
            return new Compiled(table.topic(), sql.toString(), List.copyOf(parameters), limit);
        }

        private boolean renderSelectList(StringBuilder sql) {
            List<String> items = new ArrayList<>();
            int aggregates = 0;
            for (SqlNode item : parsed.select().getSelectList()) {
                String alias = null;
                if (item instanceof SqlBasicCall as && as.getKind() == SqlKind.AS) {
                    alias = ((SqlIdentifier) as.operand(1)).getSimple();
                    item = as.operand(0);
                }
                String rendered;
                if (item instanceof SqlIdentifier id && id.isStar()) {
                    if (alias != null) {
                        throw QueryException.invalid("* cannot have an alias");
                    }
                    rendered = table.columns().stream().filter(c -> !c.system())
                            .map(c -> ServingStore.quote(c.name())).collect(Collectors.joining(", "));
                } else if (item instanceof SqlIdentifier id) {
                    rendered = ServingStore.quote(column(id).name());
                } else if (item instanceof SqlCall call && aggregateName(call) != null) {
                    aggregates++;
                    rendered = aggregate(call);
                    if (alias == null) {
                        alias = call.getOperator().getName().toLowerCase(Locale.ROOT);
                    }
                } else {
                    throw QueryException.invalid("unsupported select item " + item
                            + "; select columns, *, or COUNT/SUM/AVG/MIN/MAX without GROUP BY");
                }
                if (alias != null) {
                    rendered += " AS " + ServingStore.quote(alias);
                    outputAliases.add(alias);
                }
                items.add(rendered);
            }
            if (aggregates > 0 && aggregates != items.size()) {
                throw QueryException.invalid("aggregate functions cannot be mixed with plain columns (GROUP BY is not supported)");
            }
            sql.append(String.join(", ", items));
            return aggregates > 0;
        }

        /** The aggregate's name, or null. The parser leaves functions unresolved, so match by name. */
        private static String aggregateName(SqlCall call) {
            String name = call.getOperator().getName().toUpperCase(Locale.ROOT);
            return AGGREGATES.contains(name) ? name : null;
        }

        private String aggregate(SqlCall call) {
            String fn = aggregateName(call);
            if (call.getFunctionQuantifier() != null) {
                throw QueryException.invalid(fn + "(DISTINCT ...) is not supported");
            }
            if (call.operandCount() != 1 || !(call.operand(0) instanceof SqlIdentifier arg)) {
                throw QueryException.invalid(fn + " takes a single column (or * for COUNT)");
            }
            if (arg.isStar()) {
                if (!fn.equals("COUNT")) {
                    throw QueryException.invalid(fn + "(*) is not valid");
                }
                return "COUNT(*)";
            }
            Column col = column(arg);
            if (fn.equals("SUM") || fn.equals("AVG")) {
                if (!List.of(ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.REAL, ColumnType.DOUBLE, ColumnType.NUMERIC)
                        .contains(col.type())) {
                    throw QueryException.invalid(fn + " needs a numeric column; " + col.name() + " is " + col.type().sqlType());
                }
            } else if (!col.type().comparable() && !fn.equals("COUNT")) {
                throw QueryException.invalid(fn + " is not supported on " + col.type().sqlType() + " columns");
            }
            return fn + "(" + ServingStore.quote(col.name()) + ")";
        }

        private String condition(SqlNode node) {
            if (!(node instanceof SqlCall call)) {
                throw QueryException.invalid("unsupported condition " + node);
            }
            return switch (call.getKind()) {
                case AND, OR -> "(" + call.getOperandList().stream().map(this::condition)
                        .collect(Collectors.joining(" " + call.getKind().name() + " ")) + ")";
                case NOT -> "NOT (" + condition(call.operand(0)) + ")";
                case EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL ->
                        comparison(call.getKind(), call.operand(0), call.operand(1));
                case BETWEEN -> {
                    SqlBetweenOperator op = (SqlBetweenOperator) call.getOperator();
                    Column col = comparableColumn(call.operand(0));
                    yield ServingStore.quote(col.name()) + (op.isNegated() ? " NOT" : "") + " BETWEEN "
                            + bind(col, call.operand(1)) + " AND " + bind(col, call.operand(2));
                }
                case IN, NOT_IN -> {
                    Column col = comparableColumn(call.operand(0));
                    if (!(call.operand(1) instanceof SqlNodeList values) || values.isEmpty()) {
                        throw QueryException.invalid("IN takes a non-empty list of literals");
                    }
                    yield ServingStore.quote(col.name()) + (call.getKind() == SqlKind.NOT_IN ? " NOT IN (" : " IN (")
                            + values.stream().map(v -> bind(col, v)).collect(Collectors.joining(", ")) + ")";
                }
                case LIKE -> {
                    SqlLikeOperator op = (SqlLikeOperator) call.getOperator();
                    if (call.operandCount() != 2) {
                        throw QueryException.invalid("LIKE ... ESCAPE is not supported");
                    }
                    Column col = column(call.operand(0));
                    String target = col.type() == ColumnType.TEXT ? ServingStore.quote(col.name())
                            : "CAST(" + ServingStore.quote(col.name()) + " AS text)";
                    parameters.add(literal(call.operand(1)));
                    yield target + (op.isNegated() ? " NOT" : "") + (op.getName().equalsIgnoreCase("ILIKE") ? " ILIKE ?" : " LIKE ?");
                }
                case IS_NULL, IS_NOT_NULL -> ServingStore.quote(column(call.operand(0)).name())
                        + (call.getKind() == SqlKind.IS_NULL ? " IS NULL" : " IS NOT NULL");
                case IS_TRUE, IS_FALSE, IS_NOT_TRUE, IS_NOT_FALSE -> {
                    Column col = column(call.operand(0));
                    if (col.type() != ColumnType.BOOLEAN) {
                        throw QueryException.invalid(col.name() + " is not a boolean column");
                    }
                    yield ServingStore.quote(col.name()) + " " + call.getKind().sql.replace('_', ' ');
                }
                default -> throw QueryException.invalid("unsupported condition " + call
                        + "; use =, <>, <, <=, >, >=, BETWEEN, IN, LIKE, IS NULL combined with AND, OR, NOT");
            };
        }

        private String comparison(SqlKind kind, SqlNode left, SqlNode right) {
            if (left instanceof SqlIdentifier && right instanceof SqlIdentifier) {
                return ServingStore.quote(comparableColumn(left).name()) + " " + kind.sql + " "
                        + ServingStore.quote(comparableColumn(right).name());
            }
            if (!(left instanceof SqlIdentifier)) {
                if (!(right instanceof SqlIdentifier)) {
                    throw QueryException.invalid("comparisons need a column on one side");
                }
                return comparison(kind.reverse(), right, left);
            }
            Column col = comparableColumn(left);
            if (right instanceof SqlLiteral lit && lit.getTypeName() == SqlTypeName.NULL) {
                throw QueryException.invalid("comparing with NULL never matches; use " + col.name() + " IS NULL");
            }
            return ServingStore.quote(col.name()) + " " + kind.sql + " " + bind(col, right);
        }

        private String orderItem(SqlNode node) {
            String suffix = "";
            if (node instanceof SqlCall call && (call.getKind() == SqlKind.NULLS_FIRST || call.getKind() == SqlKind.NULLS_LAST)) {
                suffix = call.getKind() == SqlKind.NULLS_FIRST ? " NULLS FIRST" : " NULLS LAST";
                node = call.operand(0);
            }
            String direction = "";
            if (node instanceof SqlCall call && call.getKind() == SqlKind.DESCENDING) {
                direction = " DESC";
                node = call.operand(0);
            }
            if (!(node instanceof SqlIdentifier id)) {
                throw QueryException.invalid("ORDER BY takes column names");
            }
            if (id.isSimple() && outputAliases.contains(id.getSimple())) {
                return ServingStore.quote(id.getSimple()) + direction + suffix;
            }
            return ServingStore.quote(comparableColumn(id).name()) + direction + suffix;
        }

        private int limit() {
            if (parsed.fetch() == null) {
                return Math.min(defaultRows, maxRows);
            }
            int limit = integer(parsed.fetch(), "LIMIT");
            if (limit > maxRows) {
                throw QueryException.invalid("LIMIT must be at most " + maxRows);
            }
            return limit;
        }

        private int integer(SqlNode node, String clause) {
            if (node instanceof SqlNumericLiteral n && n.isInteger()) {
                long v = n.longValue(true);
                if (v >= 0 && v <= Integer.MAX_VALUE) {
                    return (int) v;
                }
            }
            throw QueryException.invalid(clause + " takes a non-negative integer");
        }

        /** Adds a literal as a parameter cast to the column's type and returns its placeholder. */
        private String bind(Column col, SqlNode literalNode) {
            parameters.add(literal(literalNode));
            return "CAST(? AS " + col.type().sqlType() + ")";
        }

        private Column comparableColumn(SqlNode node) {
            Column col = column(node);
            if (!col.type().comparable()) {
                throw QueryException.invalid(col.name() + " is a " + col.type().sqlType() + " column and cannot be compared");
            }
            return col;
        }

        private Column column(SqlNode node) {
            if (!(node instanceof SqlIdentifier id) || id.isStar()) {
                throw QueryException.invalid("expected a column name, got " + node);
            }
            List<String> names = id.names;
            if (names.size() > 1) {
                String qualifier = String.join(".", names.subList(0, names.size() - 1));
                boolean known = qualifier.equals(parsed.alias()) || qualifier.equalsIgnoreCase(table.topic());
                if (!known) {
                    throw QueryException.invalid("unknown qualifier " + qualifier + " in " + id);
                }
            }
            String name = names.getLast();
            return resolve(name, table.columns().stream().map(Column::name).toList())
                    .flatMap(table::column)
                    .orElseThrow(() -> QueryException.invalid("unknown column " + name + " in " + table.topic() + "; columns are "
                            + table.columns().stream().map(Column::name).collect(Collectors.joining(", "))));
        }
    }

    private static String literal(SqlNode node) {
        if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.MINUS_PREFIX
                && call.operand(0) instanceof SqlNumericLiteral n) {
            return "-" + n.bigDecimalValue().toPlainString();
        }
        if (node instanceof SqlNumericLiteral n) {
            return n.bigDecimalValue().toPlainString();
        }
        if (node instanceof SqlCharStringLiteral s) {
            return s.getValueAs(String.class);
        }
        if (node instanceof SqlAbstractDateTimeLiteral dt) {
            return dt.toFormattedString();
        }
        if (node instanceof SqlUnknownLiteral typed) {
            return typed.getValue(); // e.g. TIMESTAMP '2026-09-01 00:00:00', resolved by the cast to the column type
        }
        if (node instanceof SqlLiteral lit && lit.getTypeName() == SqlTypeName.BOOLEAN) {
            return String.valueOf(lit.booleanValue());
        }
        throw QueryException.invalid("expected a literal value, got " + node);
    }

    /** Exact match first, then a unique case-insensitive match. */
    static Optional<String> resolve(String requested, Collection<String> candidates) {
        if (candidates.contains(requested)) {
            return Optional.of(requested);
        }
        List<String> matches = candidates.stream().filter(c -> c.equalsIgnoreCase(requested)).toList();
        return matches.size() == 1 ? Optional.of(matches.getFirst()) : Optional.empty();
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.strip();
        return trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }

    private static String firstLine(String message) {
        int nl = message.indexOf('\n');
        return nl < 0 ? message : message.substring(0, nl);
    }
}

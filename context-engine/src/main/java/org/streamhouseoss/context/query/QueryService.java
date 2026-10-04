package org.streamhouseoss.context.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.streamhouseoss.context.ContextConfig;
import org.streamhouseoss.context.ingest.MaterializerManager;
import org.streamhouseoss.context.security.Access;
import org.streamhouseoss.context.security.AuditLog;
import org.streamhouseoss.context.store.ServingStore;
import org.streamhouseoss.context.store.TableInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;

/** Runs lightning queries and serves table metadata, enforcing grants and auditing every call. */
@ApplicationScoped
public class QueryService {

    public record ResultColumn(String name, String type) {
    }

    public record Freshness(OffsetDateTime lastRecordTimestamp, OffsetDateTime lastIngestedAt, Long lagRecords) {
    }

    public record QueryResult(String table, List<ResultColumn> columns, List<List<Object>> rows, int rowCount,
            boolean truncated, Freshness freshness, long elapsedMs, String note) {
    }

    public record TableSummary(String name, String description, String mode, String status, OffsetDateTime lastRecordTimestamp) {
    }

    public record ColumnInfo(String name, String type, boolean nullable, boolean key, String description) {
    }

    public record TableMetadata(String name, String description, String mode, String status, String statusMessage,
            List<String> keyColumns, List<ColumnInfo> columns, Freshness freshness, String exampleQuery) {
    }

    private final Access access;
    private final AuditLog audit;
    private final ServingStore store;
    private final MaterializerManager materializers;
    private final ObjectMapper mapper;
    private final LightningQueryCompiler compiler;
    private final ContextConfig config;
    private final Map<AuditLog.Channel, Timer> timers = new EnumMap<>(AuditLog.Channel.class);

    public QueryService(Access access, AuditLog audit, ServingStore store, MaterializerManager materializers,
            ObjectMapper mapper, ContextConfig config, MeterRegistry meters) {
        this.access = access;
        this.audit = audit;
        this.store = store;
        this.materializers = materializers;
        this.mapper = mapper;
        this.config = config;
        for (AuditLog.Channel channel : AuditLog.Channel.values()) {
            timers.put(channel, Timer.builder("streamhouse.context.query").tag("channel", channel.name()).register(meters));
        }
        this.compiler = new LightningQueryCompiler(config.maxRows(), config.defaultRows());
    }

    public List<TableSummary> listTables(Access.Caller caller) {
        return access.visibleTables(caller).stream()
                .map(t -> new TableSummary(t.topic(), t.description(), t.mode().name().toLowerCase(), t.status().name(),
                        t.lastRecordTimestamp()))
                .toList();
    }

    public TableMetadata metadata(Access.Caller caller, String name) {
        Map<String, TableInfo> visible = visible(caller);
        TableInfo t = LightningQueryCompiler.resolve(name, visible.keySet()).map(visible::get)
                .orElseThrow(() -> new QueryException(QueryException.Reason.NOT_FOUND,
                        "table " + name + " does not exist or you are not allowed to query it"));
        List<ColumnInfo> columns = t.columns().stream()
                .map(c -> new ColumnInfo(c.name(), c.type().sqlType(), c.nullable(), t.keyColumns().contains(c.name()), c.doc()))
                .toList();
        String where = t.keyColumns().isEmpty() ? "" : " WHERE " + t.keyColumns().stream()
                .map(k -> quoteIfNeeded(k) + " = ...").collect(Collectors.joining(" AND "));
        String example = "SELECT * FROM " + quoteIfNeeded(t.topic()) + where + " LIMIT 10";
        return new TableMetadata(t.topic(), t.description(), t.mode().name().toLowerCase(), t.status().name(),
                t.statusMessage(), t.keyColumns(), columns, freshness(t), example);
    }

    public QueryResult query(Access.Caller caller, AuditLog.Channel channel, String sql) {
        long start = System.nanoTime();
        String topic = null;
        try {
            Map<String, TableInfo> visible = visible(caller);
            LightningQueryCompiler.Parsed parsed = compiler.parse(sql, visible.keySet());
            topic = parsed.table();
            TableInfo table = visible.get(topic);
            QueryResult result;
            if (!table.materialized()) {
                result = new QueryResult(topic, List.of(), List.of(), 0, false, freshness(table), elapsedMs(start),
                        "no data has been materialized for this table yet (status " + table.status() + ")");
            } else {
                result = execute(compiler.compile(parsed, table), table, start);
            }
            audit.record(new AuditLog.Entry(channel, caller, topic, sql, AuditLog.Outcome.OK, result.rowCount(), result.elapsedMs(), null));
            timers.get(channel).record(Duration.ofNanos(System.nanoTime() - start));
            return result;
        } catch (RuntimeException e) {
            audit.record(new AuditLog.Entry(channel, caller, topic, sql, outcome(e), null, elapsedMs(start), e.getMessage()));
            throw e;
        }
    }

    private static AuditLog.Outcome outcome(RuntimeException e) {
        if (!(e instanceof QueryException q)) {
            return AuditLog.Outcome.ERROR;
        }
        return switch (q.reason()) {
            case NOT_FOUND -> AuditLog.Outcome.DENIED; // unknown and forbidden tables look the same
            case INVALID -> AuditLog.Outcome.INVALID;
            case TIMEOUT -> AuditLog.Outcome.ERROR;
        };
    }

    private Map<String, TableInfo> visible(Access.Caller caller) {
        return access.visibleTables(caller).stream().collect(Collectors.toMap(TableInfo::topic, Function.identity()));
    }

    private QueryResult execute(LightningQueryCompiler.Compiled compiled, TableInfo table, long start) {
        try (Connection c = store.connection()) {
            c.setAutoCommit(false);
            c.setReadOnly(true);
            try (var st = c.createStatement()) {
                st.execute("SET LOCAL statement_timeout = " + config.queryTimeout().toMillis());
            }
            try (PreparedStatement ps = c.prepareStatement(compiled.sql())) {
                for (int i = 0; i < compiled.parameters().size(); i++) {
                    ps.setString(i + 1, compiled.parameters().get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    ResultSetMetaData md = rs.getMetaData();
                    List<ResultColumn> columns = new ArrayList<>();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        columns.add(new ResultColumn(md.getColumnLabel(i), md.getColumnTypeName(i)));
                    }
                    List<List<Object>> rows = new ArrayList<>();
                    boolean truncated = false;
                    while (rs.next()) {
                        if (rows.size() == compiled.limit()) {
                            truncated = true;
                            break;
                        }
                        List<Object> row = new ArrayList<>(columns.size());
                        for (int i = 1; i <= columns.size(); i++) {
                            row.add(value(rs, i, columns.get(i - 1).type()));
                        }
                        rows.add(row);
                    }
                    c.commit();
                    return new QueryResult(table.topic(), columns, rows, rows.size(), truncated, freshness(table),
                            elapsedMs(start), truncated ? "more rows match; add filters or raise LIMIT (max " + config.maxRows() + ")" : null);
                }
            }
        } catch (SQLException e) {
            if ("57014".equals(e.getSQLState())) {
                throw new QueryException(QueryException.Reason.TIMEOUT,
                        "query exceeded " + config.queryTimeout().toMillis() + " ms; filter on key columns " + table.keyColumns());
            }
            if (e.getSQLState() != null && (e.getSQLState().startsWith("22") || e.getSQLState().startsWith("42"))) {
                // Data exceptions, e.g. a literal that cannot be cast to the column type.
                throw QueryException.invalid(e.getMessage().replaceFirst("^ERROR: ", ""));
            }
            throw new IllegalStateException("query failed: " + e.getMessage(), e);
        }
    }

    private Object value(ResultSet rs, int i, String type) throws SQLException {
        Object v = switch (type) {
            case "timestamptz" -> rs.getObject(i, OffsetDateTime.class);
            case "timestamp" -> rs.getObject(i, LocalDateTime.class);
            case "date" -> rs.getObject(i, LocalDate.class);
            case "time" -> rs.getObject(i, LocalTime.class);
            case "jsonb", "json" -> {
                String json = rs.getString(i);
                try {
                    yield json == null ? null : mapper.readTree(json);
                } catch (JsonProcessingException e) {
                    yield json;
                }
            }
            default -> rs.getObject(i);
        };
        if (v instanceof OffsetDateTime || v instanceof LocalDateTime || v instanceof LocalDate || v instanceof LocalTime) {
            return v.toString(); // ISO-8601, independent of the JSON mapper's date settings
        }
        return v;
    }

    private Freshness freshness(TableInfo t) {
        return new Freshness(t.lastRecordTimestamp(), t.lastIngestedAt(), materializers.lag(t.topic()).orElse(null));
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static String quoteIfNeeded(String name) {
        return name.matches("[a-z_][a-z0-9_.]*") ? name : '"' + name.replace("\"", "\"\"") + '"';
    }
}

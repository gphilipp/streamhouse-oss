package org.streamhouseoss.context.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.streamhouseoss.context.schema.Column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * JDBC access to the {@code serving} schema: table definitions, consumer offsets, grants, and
 * the DDL of materialized tables. Data writes happen in {@code Materializer} transactions using
 * the statements built here.
 */
@ApplicationScoped
public class ServingStore {

    public static final String SCHEMA = "serving";
    /** Postgres truncates identifiers longer than this, so longer topics cannot be served. */
    public static final int MAX_TABLE_NAME_LENGTH = 63;

    private static final TypeReference<List<Column>> COLUMN_LIST = new TypeReference<>() {
    };
    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() {
    };

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public ServingStore(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    public Connection connection() throws SQLException {
        return dataSource.getConnection();
    }

    public static String quote(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    public static String qualified(String topic) {
        return SCHEMA + "." + quote(topic);
    }

    // ---- table definitions ------------------------------------------------------------------

    public List<TableInfo> tables() {
        return query("SELECT * FROM serving._tables ORDER BY topic");
    }

    public Optional<TableInfo> table(String topic) {
        return query("SELECT * FROM serving._tables WHERE topic = ?", topic).stream().findFirst();
    }

    /**
     * Enables a topic. If the topic was enabled with a different mode, its materialized data is
     * dropped and it is rebuilt from the beginning of the topic.
     *
     * @return the definition after the change
     */
    public TableInfo enable(String topic, TableMode mode, String description) {
        try (Connection c = connection()) {
            c.setAutoCommit(false);
            Optional<TableInfo> existing = lock(c, topic);
            if (existing.isPresent() && existing.get().mode() != mode) {
                drop(c, topic);
                existing = Optional.empty();
            }
            if (existing.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO serving._tables (topic, mode, description, status) VALUES (?, ?, ?, ?)")) {
                    ps.setString(1, topic);
                    ps.setString(2, mode.name());
                    ps.setString(3, description);
                    ps.setString(4, TableStatus.WAITING_FOR_DATA.name());
                    ps.executeUpdate();
                }
            } else {
                // Re-enabling clears a FAILED status so the materializer retries.
                TableStatus status = existing.get().materialized() ? TableStatus.ACTIVE : TableStatus.WAITING_FOR_DATA;
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE serving._tables SET description = ?, status = ?, status_message = NULL, updated_at = now() WHERE topic = ?")) {
                    ps.setString(1, description);
                    ps.setString(2, status.name());
                    ps.setString(3, topic);
                    ps.executeUpdate();
                }
            }
            c.commit();
        } catch (SQLException e) {
            throw new StoreException("cannot enable " + topic, e);
        }
        return table(topic).orElseThrow();
    }

    /** Drops the materialized table and forgets the topic. Grants are kept. */
    public boolean disable(String topic) {
        try (Connection c = connection()) {
            c.setAutoCommit(false);
            boolean existed = lock(c, topic).isPresent();
            drop(c, topic);
            c.commit();
            return existed;
        } catch (SQLException e) {
            throw new StoreException("cannot disable " + topic, e);
        }
    }

    public void setStatus(String topic, TableStatus status, String message) {
        update("UPDATE serving._tables SET status = ?, status_message = ?, updated_at = now() WHERE topic = ?",
                status.name(), message, topic);
    }

    // ---- used inside materializer transactions ------------------------------------------------

    /** Creates the serving table, or adds new columns to it, and records the new layout. */
    public void applyLayout(Connection c, String topic, TableMode mode, List<String> keyColumns,
            List<Column> current, List<Column> target) throws SQLException {
        try (var st = c.createStatement()) {
            if (current.isEmpty()) {
                String columns = target.stream()
                        .map(col -> quote(col.name()) + " " + col.type().sqlType()
                                + (keyColumns.contains(col.name()) || !col.nullable() && col.system() ? " NOT NULL" : ""))
                        .collect(Collectors.joining(", "));
                List<String> pk = mode == TableMode.UPSERT ? keyColumns : List.of("_partition", "_offset");
                st.execute("CREATE TABLE " + qualified(topic) + " (" + columns + ", PRIMARY KEY ("
                        + pk.stream().map(ServingStore::quote).collect(Collectors.joining(", ")) + "))");
                if (target.stream().anyMatch(col -> col.name().equals("_timestamp"))) {
                    st.execute("CREATE INDEX ON " + qualified(topic) + " (" + quote("_timestamp") + ")");
                }
            } else {
                Set<String> existing = current.stream().map(Column::name).collect(Collectors.toSet());
                for (Column col : target) {
                    if (!existing.contains(col.name())) {
                        st.execute("ALTER TABLE " + qualified(topic) + " ADD COLUMN " + quote(col.name()) + " " + col.type().sqlType());
                    }
                }
            }
        }
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE serving._tables SET key_columns = CAST(? AS jsonb), columns = CAST(? AS jsonb), status = ?, status_message = NULL, updated_at = now() WHERE topic = ?")) {
            ps.setString(1, json(keyColumns));
            ps.setString(2, json(target));
            ps.setString(3, TableStatus.ACTIVE.name());
            ps.setString(4, topic);
            ps.executeUpdate();
        }
    }

    public Map<Integer, Long> offsets(String topic) {
        Map<Integer, Long> offsets = new HashMap<>();
        try (Connection c = connection();
                PreparedStatement ps = c.prepareStatement("SELECT partition, next_offset FROM serving._offsets WHERE topic = ?")) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    offsets.put(rs.getInt(1), rs.getLong(2));
                }
            }
        } catch (SQLException e) {
            throw new StoreException("cannot read offsets of " + topic, e);
        }
        return offsets;
    }

    public void saveProgress(Connection c, String topic, Map<Integer, Long> nextOffsets, OffsetDateTime lastRecordTimestamp)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO serving._offsets (topic, partition, next_offset) VALUES (?, ?, ?)
                ON CONFLICT (topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset""")) {
            for (Map.Entry<Integer, Long> e : nextOffsets.entrySet()) {
                ps.setString(1, topic);
                ps.setInt(2, e.getKey());
                ps.setLong(3, e.getValue());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE serving._tables
                SET last_record_ts = GREATEST(last_record_ts, ?), last_ingested_at = now()
                WHERE topic = ?""")) {
            ps.setObject(1, lastRecordTimestamp);
            ps.setString(2, topic);
            ps.executeUpdate();
        }
    }

    // ---- grants -------------------------------------------------------------------------------

    public void grant(String topic, String role) {
        update("INSERT INTO serving._grants (topic, role) VALUES (?, ?) ON CONFLICT DO NOTHING", topic, role);
    }

    public void revoke(String topic, String role) {
        update("DELETE FROM serving._grants WHERE topic = ? AND role = ?", topic, role);
    }

    public Map<String, Set<String>> grants() {
        Map<String, Set<String>> grants = new HashMap<>();
        try (Connection c = connection();
                PreparedStatement ps = c.prepareStatement("SELECT topic, role FROM serving._grants");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                grants.computeIfAbsent(rs.getString(1), t -> new HashSet<>()).add(rs.getString(2));
            }
        } catch (SQLException e) {
            throw new StoreException("cannot read grants", e);
        }
        return grants;
    }

    /** Topics any of the given roles may query. */
    public Set<String> grantedTopics(Collection<String> roles) {
        return grants().entrySet().stream()
                .filter(e -> e.getValue().stream().anyMatch(roles::contains))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    // ---- helpers ------------------------------------------------------------------------------

    private Optional<TableInfo> lock(Connection c, String topic) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM serving._tables WHERE topic = ? FOR UPDATE")) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(read(rs)) : Optional.empty();
            }
        }
    }

    private void drop(Connection c, String topic) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + qualified(topic));
        }
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM serving._tables WHERE topic = ?")) {
            ps.setString(1, topic);
            ps.executeUpdate();
        }
    }

    private List<TableInfo> query(String sql, Object... params) {
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            List<TableInfo> result = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(read(rs));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new StoreException("cannot read table definitions", e);
        }
    }

    private void update(String sql, Object... params) {
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("cannot update serving metadata", e);
        }
    }

    private TableInfo read(ResultSet rs) throws SQLException {
        try {
            return new TableInfo(
                    rs.getString("topic"),
                    TableMode.valueOf(rs.getString("mode")),
                    rs.getString("description"),
                    mapper.readValue(rs.getString("key_columns"), STRING_LIST),
                    mapper.readValue(rs.getString("columns"), COLUMN_LIST),
                    TableStatus.valueOf(rs.getString("status")),
                    rs.getString("status_message"),
                    rs.getObject("last_record_ts", OffsetDateTime.class),
                    rs.getObject("last_ingested_at", OffsetDateTime.class));
        } catch (JsonProcessingException e) {
            throw new StoreException("corrupt table definition " + rs.getString("topic"), e);
        }
    }

    private String json(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}

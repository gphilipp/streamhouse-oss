package org.streamhouseoss.controlplane.state;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.sql.SqlParser;
import org.streamhouseoss.sql.Statement;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Desired and observed state in the {@code streamhouse_meta} schema. A resource is stored as the
 * statement that declared it and re-parsed on read, so the SQL stays the single source of truth.
 */
@ApplicationScoped
public class DesiredState {

    public enum ApplyOutcome {
        CREATED, UPDATED, UNCHANGED
    }

    private static final TypeReference<Map<String, Object>> DETAILS = new TypeReference<>() {
    };

    private static final String SELECT = """
            SELECT r.kind, r.name, r.statement, r.generation, r.deleted,
                   s.phase, s.message, s.observed_generation, s.details, s.updated_at AS status_updated_at
            FROM streamhouse_meta.resources r
            LEFT JOIN streamhouse_meta.resource_status s ON s.kind = r.kind AND s.name = r.name""";

    @FunctionalInterface
    private interface Work<T> {
        T run(Connection c) throws SQLException;
    }

    @FunctionalInterface
    private interface RowReader<T> {
        T read(ResultSet rs) throws SQLException;
    }

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public DesiredState(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    public List<StoredResource> list(ResourceKind kind) {
        return kind == null
                ? select(SELECT + " ORDER BY r.kind, r.name", this::read)
                : select(SELECT + " WHERE r.kind = ? ORDER BY r.name", this::read, kind.name());
    }

    public Optional<StoredResource> get(ResourceKind kind, String name) {
        return select(SELECT + " WHERE r.kind = ? AND r.name = ?", this::read, kind.name(), name).stream().findFirst();
    }

    /**
     * Stores a resource. Re-declaring an identical resource is a no-op; changing an existing one
     * requires {@code orReplace} (statements that are idempotent by nature, like ALTER TOPIC and
     * GRANT, always pass it).
     */
    public ApplyOutcome apply(Resource resource, String statement, String principal, boolean orReplace) {
        String kind = resource.kind().name();
        return inTransaction("store " + label(resource), c -> {
            Optional<StoredResource> existing = selectIn(c, SELECT + " WHERE r.kind = ? AND r.name = ? FOR UPDATE OF r",
                    this::read, kind, resource.name()).stream().findFirst();
            if (existing.isEmpty()) {
                execute(c, "INSERT INTO streamhouse_meta.resources (kind, name, statement, created_by) VALUES (?, ?, ?, ?)",
                        kind, resource.name(), statement, principal);
                upsertStatus(c, resource.kind(), resource.name(), Phase.PENDING, "accepted", 0, Map.of());
                return ApplyOutcome.CREATED;
            }
            StoredResource current = existing.get();
            if (!current.deleted() && current.resource().equals(resource) && current.phase() != Phase.FAILED) {
                return ApplyOutcome.UNCHANGED;
            }
            // Changed, re-created after a drop, or re-applied after a failure (which retries it).
            if (!current.deleted() && !orReplace) {
                throw new ConflictException(label(resource) + " already exists; use CREATE OR REPLACE to change it");
            }
            execute(c, """
                    UPDATE streamhouse_meta.resources
                    SET statement = ?, generation = generation + 1, deleted = false, updated_at = now()
                    WHERE kind = ? AND name = ?""", statement, kind, resource.name());
            // Keep the observed details: reconcilers compare them with the new declaration.
            execute(c, "UPDATE streamhouse_meta.resource_status SET phase = ?, message = ?, updated_at = now() WHERE kind = ? AND name = ?",
                    Phase.PENDING.name(), "changed", kind, resource.name());
            return ApplyOutcome.UPDATED;
        });
    }

    /** Marks a resource for deletion; the reconcilers tear it down and then {@link #purge} it. */
    public boolean markDeleted(ResourceKind kind, String name) {
        return inTransaction("drop " + kind.label() + " " + name, c -> {
            int updated = execute(c, """
                    UPDATE streamhouse_meta.resources SET deleted = true, generation = generation + 1, updated_at = now()
                    WHERE kind = ? AND name = ? AND NOT deleted""", kind.name(), name);
            if (updated > 0) {
                upsertStatus(c, kind, name, Phase.DELETING, "dropping", 0, Map.of());
            }
            return updated > 0;
        });
    }

    public void purge(ResourceKind kind, String name) {
        inTransaction("purge " + kind.label() + " " + name,
                c -> execute(c, "DELETE FROM streamhouse_meta.resources WHERE kind = ? AND name = ? AND deleted", kind.name(), name));
    }

    public void setStatus(ResourceKind kind, String name, Phase phase, String message, long observedGeneration,
            Map<String, Object> details) {
        inTransaction("record the status of " + kind.label() + " " + name, c -> {
            upsertStatus(c, kind, name, phase, message, observedGeneration, details);
            return null;
        });
    }

    // ---- lineage ------------------------------------------------------------------------------

    public void replaceLineage(ResourceKind kind, String name, Collection<Edge> edges) {
        inTransaction("store the lineage of " + kind.label() + " " + name, c -> {
            execute(c, "DELETE FROM streamhouse_meta.lineage_edges WHERE via_kind = ? AND via_name = ?", kind.name(), name);
            for (Edge e : edges) {
                execute(c, """
                        INSERT INTO streamhouse_meta.lineage_edges (source_dataset, target_dataset, via_kind, via_name)
                        VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING""", e.source(), e.target(), kind.name(), name);
            }
            return null;
        });
    }

    /** The datasets a resource writes, from its recorded lineage. */
    public List<String> producedDatasets(ResourceKind kind, String name) {
        return select("SELECT DISTINCT target_dataset FROM streamhouse_meta.lineage_edges WHERE via_kind = ? AND via_name = ?",
                rs -> rs.getString(1), kind.name(), name);
    }

    /** All edges reachable upstream ({@code upstream = true}) or downstream of a dataset. */
    public List<Edge> lineage(String dataset, boolean upstream) {
        String join = upstream ? "e.target_dataset = walk.source_dataset" : "e.source_dataset = walk.target_dataset";
        String start = upstream ? "target_dataset" : "source_dataset";
        return select("""
                WITH RECURSIVE walk AS (
                    SELECT * FROM streamhouse_meta.lineage_edges WHERE %s = ?
                    UNION
                    SELECT e.* FROM streamhouse_meta.lineage_edges e JOIN walk ON %s
                ) SELECT * FROM walk""".formatted(start, join),
                rs -> new Edge(rs.getString("source_dataset"), rs.getString("target_dataset"),
                        ResourceKind.valueOf(rs.getString("via_kind")), rs.getString("via_name")),
                dataset);
    }

    // ---- audit --------------------------------------------------------------------------------

    public void audit(String principal, Collection<String> roles, String statement, String outcome, String error) {
        inTransaction("write an audit entry", c -> execute(c,
                "INSERT INTO streamhouse_meta.audit (principal, roles, statement, outcome, error) VALUES (?, ?, ?, ?, ?)",
                principal, c.createArrayOf("text", roles.toArray()), statement, outcome, error));
    }

    // ---- helpers ------------------------------------------------------------------------------

    private <T> T inTransaction(String what, Work<T> work) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            T result = work.run(c);
            c.commit();
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException("cannot " + what + ": " + e.getMessage(), e);
        }
    }

    private <T> List<T> select(String sql, RowReader<T> reader, Object... params) {
        try (Connection c = dataSource.getConnection()) {
            return selectIn(c, sql, reader, params);
        } catch (SQLException e) {
            throw new IllegalStateException("cannot read desired state: " + e.getMessage(), e);
        }
    }

    private static <T> List<T> selectIn(Connection c, String sql, RowReader<T> reader, Object... params) throws SQLException {
        try (PreparedStatement ps = prepare(c, sql, params); ResultSet rs = ps.executeQuery()) {
            List<T> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(reader.read(rs));
            }
            return rows;
        }
    }

    private static int execute(Connection c, String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = prepare(c, sql, params)) {
            return ps.executeUpdate();
        }
    }

    private static PreparedStatement prepare(Connection c, String sql, Object... params) throws SQLException {
        PreparedStatement ps = c.prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
        return ps;
    }

    private void upsertStatus(Connection c, ResourceKind kind, String name, Phase phase, String message,
            long observedGeneration, Map<String, Object> details) throws SQLException {
        try {
            execute(c, """
                    INSERT INTO streamhouse_meta.resource_status (kind, name, phase, message, observed_generation, details)
                    VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb))
                    ON CONFLICT (kind, name) DO UPDATE SET phase = EXCLUDED.phase, message = EXCLUDED.message,
                        observed_generation = EXCLUDED.observed_generation, details = EXCLUDED.details, updated_at = now()""",
                    kind.name(), name, phase.name(), message, observedGeneration, mapper.writeValueAsString(details));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private StoredResource read(ResultSet rs) throws SQLException {
        String statement = rs.getString("statement");
        Resource resource = ((Statement.Apply) SqlParser.parseStatement(statement)).resource();
        String phase = rs.getString("phase");
        String details = rs.getString("details");
        try {
            return new StoredResource(resource, statement, rs.getLong("generation"), rs.getBoolean("deleted"),
                    phase == null ? Phase.PENDING : Phase.valueOf(phase), rs.getString("message"),
                    rs.getLong("observed_generation"), details == null ? Map.of() : mapper.readValue(details, DETAILS),
                    rs.getObject("status_updated_at", OffsetDateTime.class));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String label(Resource resource) {
        return resource.kind().label() + " " + resource.name();
    }
}

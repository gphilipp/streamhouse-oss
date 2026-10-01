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

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public DesiredState(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    public List<StoredResource> list(ResourceKind kind) {
        String sql = SELECT + (kind == null ? "" : " WHERE r.kind = ?") + " ORDER BY r.kind, r.name";
        return query(sql, kind == null ? List.of() : List.of(kind.name()));
    }

    public Optional<StoredResource> get(ResourceKind kind, String name) {
        return query(SELECT + " WHERE r.kind = ? AND r.name = ?", List.of(kind.name(), name)).stream().findFirst();
    }

    /**
     * Stores a resource. Re-declaring an identical resource is a no-op; changing an existing one
     * requires {@code orReplace} (statements that are idempotent by nature, like ALTER TOPIC and
     * GRANT, always pass it).
     */
    public ApplyOutcome apply(Resource resource, String statement, String principal, boolean orReplace) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            Optional<StoredResource> existing = lockForUpdate(c, resource.kind(), resource.name());
            ApplyOutcome outcome;
            if (existing.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO streamhouse_meta.resources (kind, name, statement, created_by) VALUES (?, ?, ?, ?)")) {
                    ps.setString(1, resource.kind().name());
                    ps.setString(2, resource.name());
                    ps.setString(3, statement);
                    ps.setString(4, principal);
                    ps.executeUpdate();
                }
                upsertStatus(c, resource.kind(), resource.name(), Phase.PENDING, "accepted", 0, Map.of());
                outcome = ApplyOutcome.CREATED;
            } else if (!existing.get().deleted() && existing.get().resource().equals(resource)
                    && existing.get().phase() != Phase.FAILED) {
                outcome = ApplyOutcome.UNCHANGED;
            } else {
                // Changed, re-created after a drop, or re-applied after a failure (which retries it).
                if (!existing.get().deleted() && !orReplace) {
                    throw new ConflictException(label(resource) + " already exists; use CREATE OR REPLACE to change it");
                }
                try (PreparedStatement ps = c.prepareStatement("""
                        UPDATE streamhouse_meta.resources
                        SET statement = ?, generation = generation + 1, deleted = false, updated_at = now()
                        WHERE kind = ? AND name = ?""")) {
                    ps.setString(1, statement);
                    ps.setString(2, resource.kind().name());
                    ps.setString(3, resource.name());
                    ps.executeUpdate();
                }
                // Keep the observed details: reconcilers compare them with the new declaration.
                try (PreparedStatement ps = c.prepareStatement("""
                        UPDATE streamhouse_meta.resource_status SET phase = ?, message = ?, updated_at = now()
                        WHERE kind = ? AND name = ?""")) {
                    ps.setString(1, Phase.PENDING.name());
                    ps.setString(2, "changed");
                    ps.setString(3, resource.kind().name());
                    ps.setString(4, resource.name());
                    ps.executeUpdate();
                }
                outcome = ApplyOutcome.UPDATED;
            }
            c.commit();
            return outcome;
        } catch (SQLException e) {
            throw new IllegalStateException("cannot store " + label(resource) + ": " + e.getMessage(), e);
        }
    }

    /** Marks a resource for deletion; the reconcilers tear it down and then {@link #purge} it. */
    public boolean markDeleted(ResourceKind kind, String name) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            int updated;
            try (PreparedStatement ps = c.prepareStatement("""
                    UPDATE streamhouse_meta.resources SET deleted = true, generation = generation + 1, updated_at = now()
                    WHERE kind = ? AND name = ? AND NOT deleted""")) {
                ps.setString(1, kind.name());
                ps.setString(2, name);
                updated = ps.executeUpdate();
            }
            if (updated > 0) {
                upsertStatus(c, kind, name, Phase.DELETING, "dropping", 0, Map.of());
            }
            c.commit();
            return updated > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("cannot drop " + kind + " " + name + ": " + e.getMessage(), e);
        }
    }

    public void purge(ResourceKind kind, String name) {
        update("DELETE FROM streamhouse_meta.resources WHERE kind = ? AND name = ? AND deleted", kind.name(), name);
    }

    public void setStatus(ResourceKind kind, String name, Phase phase, String message, long observedGeneration,
            Map<String, Object> details) {
        try (Connection c = dataSource.getConnection()) {
            upsertStatus(c, kind, name, phase, message, observedGeneration, details);
        } catch (SQLException e) {
            throw new IllegalStateException("cannot record status of " + kind + " " + name + ": " + e.getMessage(), e);
        }
    }

    // ---- lineage ------------------------------------------------------------------------------

    public void replaceLineage(ResourceKind kind, String name, Collection<Edge> edges) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement del = c.prepareStatement(
                    "DELETE FROM streamhouse_meta.lineage_edges WHERE via_kind = ? AND via_name = ?")) {
                del.setString(1, kind.name());
                del.setString(2, name);
                del.executeUpdate();
            }
            try (PreparedStatement ins = c.prepareStatement("""
                    INSERT INTO streamhouse_meta.lineage_edges (source_dataset, target_dataset, via_kind, via_name)
                    VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING""")) {
                for (Edge e : edges) {
                    ins.setString(1, e.source());
                    ins.setString(2, e.target());
                    ins.setString(3, kind.name());
                    ins.setString(4, name);
                    ins.addBatch();
                }
                ins.executeBatch();
            }
            c.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("cannot store lineage of " + kind + " " + name + ": " + e.getMessage(), e);
        }
    }

    /** All edges reachable upstream ({@code upstream = true}) or downstream of a dataset. */
    public List<Edge> lineage(String dataset, boolean upstream) {
        String sql = upstream ? """
                WITH RECURSIVE up AS (
                    SELECT * FROM streamhouse_meta.lineage_edges WHERE target_dataset = ?
                    UNION
                    SELECT e.* FROM streamhouse_meta.lineage_edges e JOIN up ON e.target_dataset = up.source_dataset
                ) SELECT * FROM up""" : """
                WITH RECURSIVE down AS (
                    SELECT * FROM streamhouse_meta.lineage_edges WHERE source_dataset = ?
                    UNION
                    SELECT e.* FROM streamhouse_meta.lineage_edges e JOIN down ON e.source_dataset = down.target_dataset
                ) SELECT * FROM down""";
        List<Edge> edges = new ArrayList<>();
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, dataset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    edges.add(new Edge(rs.getString("source_dataset"), rs.getString("target_dataset"),
                            ResourceKind.valueOf(rs.getString("via_kind")), rs.getString("via_name")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("cannot read lineage of " + dataset + ": " + e.getMessage(), e);
        }
        return edges;
    }

    // ---- audit --------------------------------------------------------------------------------

    public void audit(String principal, Collection<String> roles, String statement, String outcome, String error) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO streamhouse_meta.audit (principal, roles, statement, outcome, error) VALUES (?, ?, ?, ?, ?)")) {
            ps.setString(1, principal);
            ps.setArray(2, c.createArrayOf("text", roles.toArray()));
            ps.setString(3, statement);
            ps.setString(4, outcome);
            ps.setString(5, error);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("cannot write audit entry: " + e.getMessage(), e);
        }
    }

    // ---- helpers ------------------------------------------------------------------------------

    private static final String SELECT = """
            SELECT r.kind, r.name, r.statement, r.generation, r.deleted, r.created_by, r.updated_at,
                   s.phase, s.message, s.observed_generation, s.details, s.updated_at AS status_updated_at
            FROM streamhouse_meta.resources r
            LEFT JOIN streamhouse_meta.resource_status s ON s.kind = r.kind AND s.name = r.name""";

    private Optional<StoredResource> lockForUpdate(Connection c, ResourceKind kind, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(SELECT + " WHERE r.kind = ? AND r.name = ? FOR UPDATE OF r")) {
            ps.setString(1, kind.name());
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(read(rs)) : Optional.empty();
            }
        }
    }

    private void upsertStatus(Connection c, ResourceKind kind, String name, Phase phase, String message,
            long observedGeneration, Map<String, Object> details) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO streamhouse_meta.resource_status (kind, name, phase, message, observed_generation, details)
                VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb))
                ON CONFLICT (kind, name) DO UPDATE SET phase = EXCLUDED.phase, message = EXCLUDED.message,
                    observed_generation = EXCLUDED.observed_generation, details = EXCLUDED.details, updated_at = now()""")) {
            ps.setString(1, kind.name());
            ps.setString(2, name);
            ps.setString(3, phase.name());
            ps.setString(4, message);
            ps.setLong(5, observedGeneration);
            ps.setString(6, mapper.writeValueAsString(details));
            ps.executeUpdate();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<StoredResource> query(String sql, List<String> params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setString(i + 1, params.get(i));
            }
            List<StoredResource> result = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(read(rs));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException("cannot read desired state: " + e.getMessage(), e);
        }
    }

    private void update(String sql, String... params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setString(i + 1, params[i]);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("cannot update desired state: " + e.getMessage(), e);
        }
    }

    private StoredResource read(ResultSet rs) throws SQLException {
        String statement = rs.getString("statement");
        Resource resource = ((Statement.Apply) SqlParser.parseStatement(statement)).resource();
        String phase = rs.getString("phase");
        String details = rs.getString("details");
        try {
            return new StoredResource(resource, statement, rs.getLong("generation"), rs.getBoolean("deleted"),
                    rs.getString("created_by"), rs.getObject("updated_at", OffsetDateTime.class),
                    phase == null ? Phase.PENDING : Phase.valueOf(phase), rs.getString("message"),
                    rs.getLong("observed_generation"), details == null ? Map.of() : mapper.readValue(details, DETAILS),
                    rs.getObject("status_updated_at", OffsetDateTime.class));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    static String label(Resource resource) {
        return resource.kind().name().toLowerCase().replace('_', ' ') + " " + resource.name();
    }
}

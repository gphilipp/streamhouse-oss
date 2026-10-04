package org.streamhouseoss.context.security;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.jboss.logging.Logger;
import org.streamhouseoss.context.store.ServingStore;

import jakarta.enterprise.context.ApplicationScoped;

/** Records every query, allowed or not, in {@code serving._audit}. */
@ApplicationScoped
public class AuditLog {

    private static final Logger LOG = Logger.getLogger(AuditLog.class);

    public enum Channel {
        REST, MCP
    }

    public enum Outcome {
        OK, DENIED, INVALID, ERROR
    }

    public record Entry(Channel channel, Access.Caller caller, String topic, String query, Outcome outcome,
            Integer rowCount, long elapsedMs, String error) {
    }

    private final ServingStore store;

    public AuditLog(ServingStore store) {
        this.store = store;
    }

    public void record(Entry entry) {
        try (Connection c = store.connection();
                PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO serving._audit (principal, roles, channel, topic, query, outcome, row_count, elapsed_ms, error)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""")) {
            ps.setString(1, entry.caller().principal());
            ps.setArray(2, c.createArrayOf("text", entry.caller().roles().toArray()));
            ps.setString(3, entry.channel().name());
            ps.setString(4, entry.topic());
            ps.setString(5, entry.query());
            ps.setString(6, entry.outcome().name());
            ps.setObject(7, entry.rowCount());
            ps.setInt(8, (int) Math.min(Integer.MAX_VALUE, entry.elapsedMs()));
            ps.setString(9, entry.error());
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.errorf(e, "Cannot write audit entry %s", entry);
        }
    }
}

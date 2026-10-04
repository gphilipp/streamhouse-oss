package org.streamhouseoss.controlplane.reconcile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.FlinkGateway;
import org.streamhouseoss.controlplane.flink.FlinkDdl;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Runs a named Flink SQL statement in the topic catalog, where every Kafka topic is a table.
 * Statements that start a job (INSERT INTO, CREATE TABLE ... AS SELECT) are kept running; others
 * (ALTER TABLE, CREATE TABLE) run once per generation.
 */
@ApplicationScoped
public class StatementReconciler implements Reconciler {

    private final FlinkJobSupport jobs;

    public StatementReconciler(FlinkJobSupport jobs) {
        this.jobs = jobs;
    }

    @Override
    public ResourceKind kind() {
        return ResourceKind.STATEMENT;
    }

    static String prefix(String statement) {
        return "statement-" + statement + "-";
    }

    @Override
    public Outcome reconcile(StoredResource stored, Topology topology) {
        Resource.Statement statement = (Resource.Statement) stored.resource();
        return runSql(stored, prefix(statement.name()), statement.sql());
    }

    /** Runs {@code sql} as the job {@code <prefix><hash of sql>}; shared with materialized views. */
    Outcome runSql(StoredResource stored, String prefix, String sql) {
        boolean changed = stored.observedGeneration() < stored.generation();
        if (!changed && Boolean.TRUE.equals(stored.details().get("completed"))) {
            return Outcome.ready(stored.message(), stored.details());
        }
        String jobName = prefix + FlinkJobSupport.hash(sql);
        Map<String, Object> details = new HashMap<>(Map.of("job", jobName));
        Topology.target(sql).ifPresent(t -> details.put("target", t));
        return jobs.run(stored, prefix, jobName, details, session -> {
            String jobId = submit(session, sql);
            if (jobId == null) {
                details.put("completed", true);
                return Outcome.ready("completed", details);
            }
            return Outcome.pending("submitted job " + jobId, details);
        });
    }

    /**
     * Submits the statement. A CREATE TABLE ... AS SELECT whose job stopped (e.g. Flink restarted)
     * can't re-create its table, so it resumes by inserting its query into the existing table.
     */
    private static String submit(FlinkGateway.Session session, String sql) {
        try {
            return session.execute(sql).jobId();
        } catch (ComponentException e) {
            Optional<String> query = ctasQuery(sql);
            Optional<String> target = Topology.target(sql);
            if (query.isEmpty() || target.isEmpty() || !e.getMessage().contains("already exists")) {
                throw e;
            }
            return session.execute("INSERT INTO " + FlinkDdl.quote(target.get()) + " " + query.get()).jobId();
        }
    }

    /**
     * The query of a CREATE TABLE ... AS SELECT: everything after the first AS keyword that is not
     * inside parentheses, quotes or backticks.
     */
    static Optional<String> ctasQuery(String sql) {
        if (!sql.stripLeading().regionMatches(true, 0, "CREATE", 0, 6)) {
            return Optional.empty();
        }
        int depth = 0;
        char quote = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '`' || c == '"') {
                quote = c;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && (c == 'A' || c == 'a') && sql.regionMatches(true, i, "AS", 0, 2)
                    && (i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1)))
                    && (i + 2 >= sql.length() || !Character.isLetterOrDigit(sql.charAt(i + 2)))) {
                return Optional.of(sql.substring(i + 2).strip());
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean delete(StoredResource stored, Topology topology) {
        // Like a Confluent statement deletion: the job stops, the tables it created stay.
        jobs.cancelAll(prefix(stored.name()));
        return true;
    }

    @Override
    public List<Edge> lineage(Resource resource, Topology topology) {
        return topology.lineage(resource);
    }
}

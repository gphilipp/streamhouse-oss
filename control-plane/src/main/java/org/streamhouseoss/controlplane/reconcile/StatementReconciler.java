package org.streamhouseoss.controlplane.reconcile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.streamhouseoss.controlplane.StreamhouseConfig;
import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.FlinkGateway;
import org.streamhouseoss.controlplane.flink.FlinkDdl;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.Phase;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Runs a named Flink SQL statement in the topic catalog, where every Kafka topic is a table.
 * Statements that start a job (INSERT INTO, CREATE TABLE ... AS SELECT) are tracked like views;
 * others (ALTER TABLE, CREATE TABLE) run once per generation.
 */
@ApplicationScoped
public class StatementReconciler implements Reconciler {

    private static final Pattern TARGET = Pattern.compile(
            "^\\s*(?:CREATE\\s+(?:OR\\s+REPLACE\\s+)?TABLE(?:\\s+IF\\s+NOT\\s+EXISTS)?|INSERT\\s+(?:INTO|OVERWRITE))\\s+((?:`[^`]+`|[\\w$]+)(?:\\s*\\.\\s*(?:`[^`]+`|[\\w$]+))*)",
            Pattern.CASE_INSENSITIVE);

    private final FlinkGateway gateway;
    private final FlinkJobSupport jobs;
    private final StreamhouseConfig.Flink flink;

    public StatementReconciler(FlinkGateway gateway, FlinkJobSupport jobs, StreamhouseConfig config) {
        this.gateway = gateway;
        this.jobs = jobs;
        this.flink = config.flink();
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
        String jobName = prefix(statement.name()) + FlinkJobSupport.hash(statement.sql());
        boolean changed = stored.observedGeneration() < stored.generation();
        boolean completedOnce = Boolean.TRUE.equals(stored.details().get("completed"));
        if (!changed && completedOnce) {
            return Outcome.ready(stored.message(), stored.details());
        }
        FlinkJobSupport.Observed observed = jobs.observe(prefix(statement.name()), jobName);
        if (observed.state() == FlinkJobSupport.JobState.RUNNING) {
            return FlinkJobSupport.running(observed, details(jobName, statement));
        }
        if (observed.state() == FlinkJobSupport.JobState.FAILED && !changed) {
            return Outcome.failed(observed.message());
        }
        try (FlinkGateway.Session session = gateway.open(jobName)) {
            session.execute("USE CATALOG " + FlinkDdl.quote(flink.topicCatalog()));
            session.execute("USE " + FlinkDdl.quote(flink.topicDatabase()));
            session.execute(FlinkDdl.set("pipeline.name", jobName));
            String jobId;
            try {
                jobId = session.execute(statement.sql()).jobId();
            } catch (ComponentException e) {
                // A CTAS whose job stopped (e.g. Flink restarted) can't re-create its table: resume it
                // by inserting the query into the existing table, as a restarted statement would.
                Optional<String> query = ctasQuery(statement.sql());
                Optional<String> target = target(statement.sql());
                if (query.isEmpty() || target.isEmpty() || !e.getMessage().contains("already exists")) {
                    throw e;
                }
                jobId = session.execute("INSERT INTO " + FlinkDdl.quote(target.get()) + " " + query.get()).jobId();
            }
            if (jobId == null) {
                Map<String, Object> done = new java.util.HashMap<>(details(jobName, statement));
                done.put("completed", true);
                return Outcome.ready("completed", done);
            }
            return new Outcome(Phase.PENDING, "submitted job " + jobId, details(jobName, statement));
        } catch (ComponentException e) {
            return Outcome.failed(e.getMessage());
        }
    }

    private static Map<String, Object> details(String jobName, Resource.Statement statement) {
        return target(statement.sql()).<Map<String, Object>>map(t -> Map.of("job", jobName, "target", t))
                .orElse(Map.of("job", jobName));
    }

    /** The table a statement writes to, unqualified: CREATE TABLE x ... / INSERT INTO x ... */
    public static Optional<String> target(String sql) {
        Matcher m = TARGET.matcher(sql);
        if (!m.find()) {
            return Optional.empty();
        }
        List<String> parts = new ArrayList<>();
        Matcher part = Pattern.compile("`([^`]+)`|([\\w$]+)").matcher(m.group(1));
        while (part.find()) {
            parts.add(part.group(1) != null ? part.group(1) : part.group(2));
        }
        return parts.isEmpty() ? Optional.empty() : Optional.of(parts.getLast());
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
        Resource.Statement statement = (Resource.Statement) resource;
        Optional<String> target = target(statement.sql());
        if (target.isEmpty()) {
            return List.of();
        }
        return topology.referencedTopics(statement.sql(), target.get()).stream()
                .map(t -> new Edge(Edge.kafka(t), Edge.kafka(target.get()), ResourceKind.STATEMENT, statement.name()))
                .toList();
    }
}

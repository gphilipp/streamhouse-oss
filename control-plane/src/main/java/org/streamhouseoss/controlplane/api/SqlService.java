package org.streamhouseoss.controlplane.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.streamhouseoss.controlplane.clients.KafkaTopics;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.reconcile.ReconcileLoop;
import org.streamhouseoss.controlplane.reconcile.Topology;
import org.streamhouseoss.controlplane.state.ConflictException;
import org.streamhouseoss.controlplane.state.DesiredState;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.sql.SqlParser;
import org.streamhouseoss.sql.Statement;

import io.quarkus.security.ForbiddenException;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Executes streamhouse SQL scripts. A script is parsed completely before anything runs; statements
 * then run in order and execution stops at the first error. DDL only records desired state; the
 * reconcile loop makes it real, and its progress is visible with SHOW and DESCRIBE.
 */
@ApplicationScoped
public class SqlService {

    public record StatementResult(String statement, String status, String message, List<String> columns,
            List<List<Object>> rows) {
        static StatementResult ok(String statement, String message) {
            return new StatementResult(statement, "OK", message, List.of(), List.of());
        }

        static StatementResult rows(String statement, List<String> columns, List<List<Object>> rows) {
            return new StatementResult(statement, "OK", rows.size() + " row(s)", columns, rows);
        }
    }

    public record ScriptResult(boolean ok, List<StatementResult> results) {
    }

    private static final Set<String> WRITERS = Set.of("admin", "engineer");
    private static final Set<String> GRANTORS = Set.of("admin");

    private final DesiredState state;
    private final ReconcileLoop loop;
    private final KafkaTopics topics;

    public SqlService(DesiredState state, ReconcileLoop loop, KafkaTopics topics) {
        this.state = state;
        this.loop = loop;
        this.topics = topics;
    }

    public ScriptResult execute(String script, SecurityIdentity identity) {
        List<Statement> statements = SqlParser.parseScript(script);
        String principal = identity.getPrincipal().getName();
        List<StatementResult> results = new ArrayList<>();
        boolean changed = false;
        for (Statement statement : statements) {
            try {
                StatementResult result = execute(statement, identity);
                results.add(result);
                boolean write = statement instanceof Statement.Apply || statement instanceof Statement.Remove;
                if (write) {
                    state.audit(principal, identity.getRoles(), statement.text(), "OK", null);
                    changed = true;
                }
            } catch (ConflictException | ForbiddenException | IllegalArgumentException e) {
                state.audit(principal, identity.getRoles(), statement.text(), e instanceof ForbiddenException ? "DENIED" : "INVALID",
                        e.getMessage());
                results.add(new StatementResult(statement.text(), "ERROR", e.getMessage(), List.of(), List.of()));
                int skipped = statements.size() - results.size();
                if (skipped > 0) {
                    results.add(new StatementResult("", "SKIPPED", skipped + " remaining statement(s) not executed", List.of(), List.of()));
                }
                break;
            }
        }
        if (changed) {
            loop.trigger();
        }
        return new ScriptResult(results.stream().allMatch(r -> r.status().equals("OK")), results);
    }

    private StatementResult execute(Statement statement, SecurityIdentity identity) {
        return switch (statement) {
            case Statement.Apply apply -> apply(apply, identity);
            case Statement.Remove remove -> remove(remove, identity);
            case Statement.Show show -> show(show);
            case Statement.Describe describe -> describe(describe);
        };
    }

    // ---- writes -------------------------------------------------------------------------------

    private StatementResult apply(Statement.Apply apply, SecurityIdentity identity) {
        Resource resource = apply.resource();
        requireRole(identity, resource instanceof Resource.Grant ? GRANTORS : WRITERS, apply.text());
        validate(resource, Topology.of(state.list(null)));
        DesiredState.ApplyOutcome outcome = state.apply(resource, apply.text(), identity.getPrincipal().getName(), apply.orReplace());
        String what = resource.kind().label() + " " + display(resource);
        return StatementResult.ok(apply.text(), switch (outcome) {
            case CREATED -> what + " created";
            case UPDATED -> what + " updated";
            case UNCHANGED -> what + " unchanged";
        });
    }

    private void validate(Resource resource, Topology topology) {
        switch (resource) {
            case Resource.Connection c -> {
                List<String> missing = Resource.Connection.REQUIRED_OPTIONS.stream()
                        .filter(k -> !c.options().containsKey(k)).toList();
                if (!missing.isEmpty()) {
                    throw new IllegalArgumentException("connection " + c.name() + " is missing option(s) " + String.join(", ", missing));
                }
            }
            case Resource.Source s -> {
                if (topology.connection(s.connection()).isEmpty()) {
                    throw new ConflictException("connection " + s.connection() + " does not exist");
                }
                Set<String> views = topology.all(Resource.MaterializedView.class).map(Resource.MaterializedView::name).collect(Collectors.toSet());
                s.tables().stream().map(s::topicFor).filter(views::contains).findFirst().ifPresent(t -> {
                    throw new ConflictException("topic " + t + " is already produced by a materialized view");
                });
            }
            case Resource.MaterializedView mv -> {
                if (topology.producedTopics().contains(mv.name())
                        && topology.find(ResourceKind.MATERIALIZED_VIEW, mv.name()).isEmpty()) {
                    throw new ConflictException("topic " + mv.name() + " is already produced by a source");
                }
                if (topology.referencedTopics(mv.query(), mv.name()).isEmpty()) {
                    throw new IllegalArgumentException("the query must read at least one declared topic (quote dotted names "
                            + "with backticks); known topics: " + String.join(", ", topology.producedTopics()));
                }
            }
            case Resource.Statement st -> {
                if (st.sql().isBlank()) {
                    throw new IllegalArgumentException("statement " + st.name() + " has no SQL");
                }
            }
            case Resource.IcebergTable it -> requireTopic(it.name(), topology);
            case Resource.ContextTable ct -> requireTopic(ct.name(), topology);
            case Resource.Grant g -> {
                if (topology.find(ResourceKind.CONTEXT_TABLE, g.objectName()).isEmpty()) {
                    throw new ConflictException("context is not enabled on topic " + g.objectName()
                            + "; run ALTER TOPIC " + g.objectName() + " ENABLE CONTEXT first");
                }
            }
        }
    }

    private void requireTopic(String topic, Topology topology) {
        if (!topology.producedTopics().contains(topic) && !topics.exists(topic)) {
            throw new ConflictException("topic " + topic + " does not exist and no source or materialized view produces it");
        }
    }

    private StatementResult remove(Statement.Remove remove, SecurityIdentity identity) {
        requireRole(identity, remove.kind() == ResourceKind.GRANT ? GRANTORS : WRITERS, remove.text());
        Topology topology = Topology.of(state.list(null));
        Optional<StoredResource> existing = topology.find(remove.kind(), remove.name());
        if (existing.isEmpty()) {
            if (remove.ifExists()) {
                return StatementResult.ok(remove.text(), remove.kind().label() + " " + remove.name() + " does not exist; nothing to do");
            }
            throw new ConflictException(remove.kind().label() + " " + remove.name() + " does not exist");
        }
        List<String> dependents = dependents(existing.get().resource(), topology);
        if (!dependents.isEmpty()) {
            throw new ConflictException("cannot drop " + remove.kind().label() + " " + remove.name() + ": used by "
                    + String.join(", ", dependents));
        }
        state.markDeleted(remove.kind(), remove.name());
        return StatementResult.ok(remove.text(), remove.kind().label() + " " + display(existing.get().resource()) + " dropped");
    }

    private List<String> dependents(Resource resource, Topology topology) {
        List<String> dependents = new ArrayList<>();
        if (resource instanceof Resource.Connection c) {
            topology.all(Resource.Source.class).filter(s -> s.connection().equals(c.name()))
                    .forEach(s -> dependents.add("source " + s.name()));
        }
        for (String topic : Topology.producedBy(resource)) {
            topology.consumersOf(topic).stream()
                    .filter(r -> !(r.resource().equals(resource)))
                    .forEach(r -> dependents.add(r.kind().label() + " " + r.name()));
        }
        return dependents.stream().distinct().toList();
    }

    // ---- reads --------------------------------------------------------------------------------

    private StatementResult show(Statement.Show show) {
        if (show.kind() == null) {
            Topology topology = Topology.of(state.list(null));
            Map<String, String> producers = new LinkedHashMap<>();
            topology.resources().forEach(r -> Topology.producedBy(r.resource())
                    .forEach(t -> producers.put(t, r.kind().label() + " " + r.name())));
            List<List<Object>> rows = topics.list().stream()
                    .filter(t -> !t.startsWith("_") && !t.startsWith("connect-") && !t.startsWith("__"))
                    .sorted()
                    .map(t -> List.<Object>of(t, producers.getOrDefault(t, ""),
                            topology.consumersOf(t).stream().map(r -> r.kind().label() + " " + r.name()).collect(Collectors.joining(", "))))
                    .toList();
            return StatementResult.rows(show.text(), List.of("topic", "produced_by", "used_by"), rows);
        }
        List<List<Object>> rows = state.list(show.kind()).stream()
                .map(r -> List.<Object>of(display(r.resource()), r.phase().name(), r.message() == null ? "" : r.message()))
                .toList();
        return StatementResult.rows(show.text(), List.of("name", "status", "message"), rows);
    }

    private StatementResult describe(Statement.Describe describe) {
        StoredResource r = state.get(describe.kind(), describe.name())
                .orElseThrow(() -> new ConflictException(describe.kind().label() + " " + describe.name() + " does not exist"));
        List<List<Object>> rows = new ArrayList<>();
        rows.add(List.of("name", display(r.resource())));
        rows.add(List.of("status", r.phase().name()));
        rows.add(List.of("message", r.message() == null ? "" : r.message()));
        rows.add(List.of("generation", r.generation() + " (observed " + r.observedGeneration() + ")"));
        rows.add(List.of("statement", r.statement()));
        r.details().forEach((k, v) -> rows.add(List.of(k, String.valueOf(v))));
        List<Edge> upstream = new ArrayList<>();
        List<Edge> downstream = new ArrayList<>();
        for (String dataset : state.producedDatasets(r.kind(), r.name())) {
            upstream.addAll(state.lineage(dataset, true));
            downstream.addAll(state.lineage(dataset, false));
        }
        rows.add(List.of("upstream", formatEdges(upstream)));
        rows.add(List.of("downstream", formatEdges(downstream)));
        return StatementResult.rows(describe.text(), List.of("property", "value"), rows);
    }

    private static String formatEdges(List<Edge> edges) {
        return edges.stream().map(e -> e.source() + " -> " + e.target()).distinct().collect(Collectors.joining("; "));
    }

    // ---- helpers ------------------------------------------------------------------------------

    private static void requireRole(SecurityIdentity identity, Set<String> roles, String statement) {
        if (identity.getRoles().stream().noneMatch(roles::contains)) {
            throw new ForbiddenException("requires one of the roles " + roles.stream().sorted().toList() + " to run: "
                    + statement.lines().findFirst().orElse(statement));
        }
    }

    static String display(Resource resource) {
        return resource instanceof Resource.Grant g
                ? g.privilege() + " ON CONTEXT " + g.objectName() + " TO ROLE " + g.role()
                : resource.name();
    }
}

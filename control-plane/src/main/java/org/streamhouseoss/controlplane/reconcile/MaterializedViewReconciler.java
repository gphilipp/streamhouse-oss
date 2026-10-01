package org.streamhouseoss.controlplane.reconcile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.FlinkGateway;
import org.streamhouseoss.controlplane.clients.KafkaTopics;
import org.streamhouseoss.controlplane.clients.SchemaRegistry;
import org.streamhouseoss.controlplane.flink.FlinkDdl;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * A materialized view is a Flink SQL job that reads the topics its query references (as
 * changelogs) and upserts the result, keyed by the declared primary key, into a compacted topic
 * named after the view. The result's Avro schemas are registered by the job itself.
 */
@ApplicationScoped
public class MaterializedViewReconciler implements Reconciler {

    private static final String VIEW = "__streamhouse_view";
    private static final String SINK = "__streamhouse_sink";

    private final FlinkGateway gateway;
    private final FlinkJobSupport jobs;
    private final FlinkDdl ddl;
    private final TopicSchemas schemas;
    private final KafkaTopics topics;
    private final SchemaRegistry registry;

    public MaterializedViewReconciler(FlinkGateway gateway, FlinkJobSupport jobs, FlinkDdl ddl, TopicSchemas schemas,
            KafkaTopics topics, SchemaRegistry registry) {
        this.gateway = gateway;
        this.jobs = jobs;
        this.ddl = ddl;
        this.schemas = schemas;
        this.topics = topics;
        this.registry = registry;
    }

    @Override
    public ResourceKind kind() {
        return ResourceKind.MATERIALIZED_VIEW;
    }

    static String prefix(String view) {
        return "mv-" + view + "-";
    }

    @Override
    public Outcome reconcile(StoredResource stored, Topology topology) {
        Resource.MaterializedView mv = (Resource.MaterializedView) stored.resource();
        List<String> inputs = topology.referencedTopics(mv.query(), mv.name());
        if (inputs.isEmpty()) {
            return Outcome.failed("the query does not read any declared topic; reference sources' topics or other views, "
                    + "e.g. FROM `shop.public.orders`");
        }
        String jobName = prefix(mv.name()) + FlinkJobSupport.hash(mv.query(), String.join(",", mv.primaryKey()));
        Map<String, Object> details = Map.of("job", jobName, "inputs", inputs, "topic", mv.name());
        FlinkJobSupport.Observed observed = jobs.observe(prefix(mv.name()), jobName);
        switch (observed.state()) {
            case RUNNING:
                return FlinkJobSupport.running(observed, details);
            case FAILED:
                if (stored.observedGeneration() >= stored.generation()) {
                    return Outcome.failed(observed.message());
                }
                break; // re-applied after a failure: submit again
            case ABSENT:
                break;
        }

        List<String> sourceDdl = new ArrayList<>();
        for (String input : inputs) {
            Optional<TopicSchemas.TopicSchema> schema = schemas.of(input);
            if (schema.isEmpty()) {
                return Outcome.pending("waiting for the schema of topic " + input + " (has it received data yet?)");
            }
            sourceDdl.add(ddl.kafkaSource(input, schema.get().columns(), schema.get().keyFields(), jobName));
        }
        topics.ensure(mv.name(), true);

        try (FlinkGateway.Session session = gateway.open(jobName)) {
            session.execute(FlinkDdl.set("table.exec.sink.not-null-enforcer", "DROP"));
            sourceDdl.forEach(session::execute);
            session.execute("CREATE TEMPORARY VIEW " + FlinkDdl.quote(VIEW) + " AS " + mv.query());
            List<FlinkDdl.FlinkColumn> columns = describe(session);
            for (String key : mv.primaryKey()) {
                if (columns.stream().noneMatch(c -> c.name().equals(key))) {
                    return Outcome.failed("primary key column " + key + " is not in the query result; columns are "
                            + columns.stream().map(FlinkDdl.FlinkColumn::name).toList());
                }
            }
            session.execute(ddl.upsertKafkaSink(SINK, mv.name(), columns, mv.primaryKey()));
            session.execute(FlinkDdl.set("pipeline.name", jobName));
            String jobId = session.execute("INSERT INTO " + FlinkDdl.quote(SINK) + " SELECT * FROM " + FlinkDdl.quote(VIEW)).jobId();
            return Outcome.pending("submitted job " + jobId);
        } catch (ComponentException e) {
            return Outcome.failed(e.getMessage());
        }
    }

    private static List<FlinkDdl.FlinkColumn> describe(FlinkGateway.Session session) {
        FlinkGateway.Result described = session.execute("DESCRIBE " + FlinkDdl.quote(VIEW));
        int name = described.columns().indexOf("name");
        int type = described.columns().indexOf("type");
        int nullable = described.columns().indexOf("null");
        List<FlinkDdl.FlinkColumn> columns = new ArrayList<>();
        for (List<JsonNode> row : described.rows()) {
            String flinkType = row.get(type).asText().replaceAll(" NOT NULL$", "");
            columns.add(new FlinkDdl.FlinkColumn(row.get(name).asText(), flinkType, row.get(nullable).asBoolean(true)));
        }
        return columns;
    }

    @Override
    public boolean delete(StoredResource stored, Topology topology) {
        Resource.MaterializedView mv = (Resource.MaterializedView) stored.resource();
        jobs.cancelAll(prefix(mv.name()));
        topics.delete(List.of(mv.name()));
        registry.deleteSubject(mv.name() + "-key");
        registry.deleteSubject(mv.name() + "-value");
        return true;
    }

    @Override
    public List<Edge> lineage(Resource resource, Topology topology) {
        Resource.MaterializedView mv = (Resource.MaterializedView) resource;
        return topology.referencedTopics(mv.query(), mv.name()).stream()
                .map(t -> new Edge(Edge.kafka(t), Edge.kafka(mv.name()), ResourceKind.MATERIALIZED_VIEW, mv.name()))
                .toList();
    }
}

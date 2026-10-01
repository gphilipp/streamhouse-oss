package org.streamhouseoss.controlplane.reconcile;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.FlinkGateway;
import org.streamhouseoss.controlplane.clients.KafkaTopics;
import org.streamhouseoss.controlplane.flink.FlinkDdl;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.model.TableMode;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Tableflow continuously lands a topic in an Iceberg table registered in the Gravitino Iceberg
 * REST catalog: every event in append mode, the latest row per key (equality deletes) in upsert
 * mode. Commits happen on Flink checkpoints. Disabling stops the job and keeps the table.
 */
@ApplicationScoped
public class TableflowReconciler implements Reconciler {

    private final FlinkGateway gateway;
    private final FlinkJobSupport jobs;
    private final FlinkDdl ddl;
    private final TopicSchemas schemas;
    private final KafkaTopics topics;

    public TableflowReconciler(FlinkGateway gateway, FlinkJobSupport jobs, FlinkDdl ddl, TopicSchemas schemas, KafkaTopics topics) {
        this.gateway = gateway;
        this.jobs = jobs;
        this.ddl = ddl;
        this.schemas = schemas;
        this.topics = topics;
    }

    @Override
    public ResourceKind kind() {
        return ResourceKind.TABLEFLOW;
    }

    static String prefix(String topic) {
        return "tableflow-" + topic + "-";
    }

    @Override
    public Outcome reconcile(StoredResource stored, Topology topology) {
        Resource.Tableflow tableflow = (Resource.Tableflow) stored.resource();
        String topic = tableflow.name();
        String table = FlinkDdl.icebergTableFor(topic);
        String mode = tableflow.mode().name().toLowerCase(Locale.ROOT);
        String jobName = prefix(topic) + mode;
        Map<String, Object> details = Map.of("job", jobName, "table", ddl.namespace() + "." + table, "mode", mode);

        FlinkJobSupport.Observed observed = jobs.observe(prefix(topic), jobName);
        if (observed.state() == FlinkJobSupport.JobState.RUNNING) {
            return FlinkJobSupport.running(observed, details);
        }
        if (observed.state() == FlinkJobSupport.JobState.FAILED && stored.observedGeneration() >= stored.generation()) {
            return Outcome.failed(observed.message());
        }
        if (!topics.exists(topic)) {
            return Outcome.pending("waiting for topic " + topic);
        }
        Optional<TopicSchemas.TopicSchema> schema = schemas.of(topic);
        if (schema.isEmpty()) {
            return Outcome.pending("waiting for the schema of topic " + topic + " (has it received data yet?)");
        }
        boolean upsert = tableflow.mode() == TableMode.UPSERT;
        List<String> keys = schema.get().keyFields();
        if (upsert && keys.isEmpty()) {
            return Outcome.failed("upsert mode needs a keyed topic with an Avro key schema; use WITH (mode = 'append')");
        }
        String previousMode = (String) stored.details().get("mode");
        try (FlinkGateway.Session session = gateway.open(jobName)) {
            session.execute(ddl.icebergCatalog());
            session.execute(ddl.icebergNamespace());
            if (previousMode != null && !previousMode.equals(mode)) {
                // Append and upsert tables have different layouts; switching modes rebuilds the table.
                session.execute("DROP TABLE IF EXISTS " + ddl.icebergTableName(table));
            }
            session.execute(ddl.icebergTable(table, schema.get().columns(), upsert ? keys : List.of()));
            session.execute(ddl.kafkaSource(topic, schema.get().columns(), upsert ? keys : List.of(), jobName));
            session.execute(FlinkDdl.set("pipeline.name", jobName));
            String jobId = session.execute("INSERT INTO " + ddl.icebergTableName(table) + " SELECT * FROM " + FlinkDdl.quote(topic)).jobId();
            return new Outcome(org.streamhouseoss.controlplane.state.Phase.PENDING, "submitted job " + jobId, details);
        } catch (ComponentException e) {
            return Outcome.failed(e.getMessage());
        }
    }

    @Override
    public boolean delete(StoredResource stored, Topology topology) {
        jobs.cancelAll(prefix(stored.name()));
        return true;
    }

    @Override
    public List<Edge> lineage(Resource resource, Topology topology) {
        String topic = resource.name();
        return List.of(new Edge(Edge.kafka(topic), Edge.iceberg(ddl.namespace(), FlinkDdl.icebergTableFor(topic)),
                ResourceKind.TABLEFLOW, topic));
    }
}

package org.streamhouseoss.controlplane.reconcile;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.StreamhouseConfig;
import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.FlinkGateway;
import org.streamhouseoss.controlplane.clients.KafkaTopics;
import org.streamhouseoss.controlplane.flink.FlinkDdl;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.Phase;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.model.TableMode;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Continuously lands a topic in an Iceberg table registered in the Gravitino Iceberg
 * REST catalog: every event in append mode, the latest row per key (equality deletes) in upsert
 * mode. Commits happen on Flink checkpoints. Disabling stops the job and keeps the table.
 */
@ApplicationScoped
public class IcebergTableReconciler implements Reconciler {

    private final FlinkGateway gateway;
    private final FlinkJobSupport jobs;
    private final FlinkDdl ddl;
    private final TopicSchemas schemas;
    private final KafkaTopics topics;
    private final StreamhouseConfig.Flink flink;

    public IcebergTableReconciler(FlinkGateway gateway, FlinkJobSupport jobs, FlinkDdl ddl, TopicSchemas schemas, KafkaTopics topics,
            StreamhouseConfig config) {
        this.gateway = gateway;
        this.jobs = jobs;
        this.ddl = ddl;
        this.schemas = schemas;
        this.topics = topics;
        this.flink = config.flink();
    }

    @Override
    public ResourceKind kind() {
        return ResourceKind.ICEBERG_TABLE;
    }

    static String prefix(String topic) {
        return "iceberg-" + topic + "-";
    }

    @Override
    public Outcome reconcile(StoredResource stored, Topology topology) {
        Resource.IcebergTable iceberg = (Resource.IcebergTable) stored.resource();
        String topic = iceberg.name();
        String table = FlinkDdl.icebergTableFor(topic);
        if (!topics.exists(topic)) {
            return Outcome.pending("waiting for topic " + topic);
        }
        // Like Confluent: compacted (keyed) topics become upsert tables, others append-only tables.
        TableMode resolved = iceberg.mode() != null ? iceberg.mode()
                : topics.compacted(topic) ? TableMode.UPSERT : TableMode.APPEND;
        String mode = resolved.name().toLowerCase(Locale.ROOT);
        String jobName = prefix(topic) + mode;
        Map<String, Object> details = Map.of("job", jobName, "table", ddl.namespace() + "." + table, "mode", mode);

        FlinkJobSupport.Observed observed = jobs.observe(prefix(topic), jobName);
        if (observed.state() == FlinkJobSupport.JobState.RUNNING) {
            return FlinkJobSupport.running(observed, details);
        }
        if (observed.state() == FlinkJobSupport.JobState.FAILED && stored.observedGeneration() >= stored.generation()) {
            return Outcome.failed(observed.message());
        }
        Optional<TopicSchemas.TopicSchema> schema = schemas.of(topic);
        if (schema.isEmpty()) {
            return Outcome.pending("waiting for the schema of topic " + topic + " (has it received data yet?)");
        }
        String previousMode = (String) stored.details().get("mode");
        try (FlinkGateway.Session session = gateway.open(jobName)) {
            session.execute(ddl.icebergCatalog());
            session.execute(ddl.icebergNamespace());
            if (previousMode != null && !previousMode.equals(mode)) {
                // Append and upsert tables have different layouts; switching modes rebuilds the table.
                session.execute("DROP TABLE IF EXISTS " + ddl.icebergTableName(table));
            }
            String select;
            if (resolved == TableMode.UPSERT) {
                // Read the changelog through the topic catalog, which understands every key format (Avro, raw).
                String source = FlinkDdl.quote(flink.topicCatalog()) + "." + FlinkDdl.quote(flink.topicDatabase()) + "." + FlinkDdl.quote(topic);
                Described described = describe(session, source);
                if (described.primaryKey().isEmpty()) {
                    return Outcome.failed("upsert mode needs a keyed, compacted topic; use WITH (mode = 'append')");
                }
                session.execute(ddl.icebergTable(table, described.columns(), described.primaryKey()));
                select = "SELECT " + described.columns().stream().map(c -> FlinkDdl.quote(c.name()))
                        .collect(java.util.stream.Collectors.joining(", ")) + " FROM " + source;
            } else {
                // Every record, including updates: read the topic as an append-only log.
                session.execute(ddl.icebergTable(table, schema.get().columns(), List.of()));
                session.execute(ddl.kafkaSource(topic, schema.get().columns(), List.of(), jobName));
                select = "SELECT * FROM " + FlinkDdl.quote(topic);
            }
            session.execute(FlinkDdl.set("pipeline.name", jobName));
            String jobId = session.execute("INSERT INTO " + ddl.icebergTableName(table) + " " + select).jobId();
            return new Outcome(Phase.PENDING, "submitted job " + jobId, details);
        } catch (ComponentException e) {
            return Outcome.failed(e.getMessage());
        }
    }

    record Described(List<FlinkDdl.FlinkColumn> columns, List<String> primaryKey) {
    }

    /** Physical columns and primary key of a table, from DESCRIBE (metadata and computed columns are skipped). */
    static Described describe(FlinkGateway.Session session, String table) {
        FlinkGateway.Result result = session.execute("DESCRIBE " + table);
        int name = result.columns().indexOf("name");
        int type = result.columns().indexOf("type");
        int nullable = result.columns().indexOf("null");
        int key = result.columns().indexOf("key");
        int extras = result.columns().indexOf("extras");
        List<FlinkDdl.FlinkColumn> columns = new java.util.ArrayList<>();
        List<String> primaryKey = new java.util.ArrayList<>();
        for (List<com.fasterxml.jackson.databind.JsonNode> row : result.rows()) {
            String extra = extras < 0 || row.get(extras).isNull() ? "" : row.get(extras).asText();
            String column = row.get(name).asText();
            if (column.startsWith("$") || extra.contains("METADATA") || extra.startsWith("AS ")) {
                continue;
            }
            columns.add(new FlinkDdl.FlinkColumn(column, row.get(type).asText().replaceAll(" NOT NULL$", ""),
                    row.get(nullable).asBoolean(true)));
            if (key >= 0 && row.get(key).asText("").startsWith("PRI")) {
                primaryKey.add(column);
            }
        }
        return new Described(columns, primaryKey);
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
                ResourceKind.ICEBERG_TABLE, topic));
    }
}

package org.streamhouseoss.controlplane.reconcile;

import java.util.List;
import java.util.Locale;
import java.util.Map;

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
 * Continuously lands a topic in an Iceberg table of the {@code lake} catalog: the latest row per
 * key (equality deletes) in upsert mode, every record in append mode. The topic is read through the
 * topic catalog, which decides its columns and formats. Commits happen on Flink checkpoints.
 * Disabling stops the job and keeps the table.
 */
@ApplicationScoped
public class IcebergTableReconciler implements Reconciler {

    private static final String LOG = "__streamhouse_log";

    private final FlinkJobSupport jobs;
    private final FlinkDdl ddl;
    private final KafkaTopics topics;

    public IcebergTableReconciler(FlinkJobSupport jobs, FlinkDdl ddl, KafkaTopics topics) {
        this.jobs = jobs;
        this.ddl = ddl;
        this.topics = topics;
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
        TableMode mode = iceberg.mode() != null ? iceberg.mode()
                : topics.compacted(topic) ? TableMode.UPSERT : TableMode.APPEND;
        String modeName = mode.name().toLowerCase(Locale.ROOT);
        String jobName = prefix(topic) + modeName;
        Map<String, Object> details = Map.of("job", jobName, "table", ddl.namespace() + "." + table, "mode", modeName);
        String previousMode = (String) stored.details().get("mode");

        return jobs.run(stored, prefix(topic), jobName, details, session -> {
            FlinkGateway.Described source;
            try {
                source = session.describe(ddl.topicTable(topic));
            } catch (ComponentException e) {
                return Outcome.pending("waiting for the schema of topic " + topic + " (has it received data yet?)", details);
            }
            if (mode == TableMode.UPSERT && source.primaryKey().isEmpty()) {
                return Outcome.failed("upsert mode needs a keyed, compacted topic; use WITH (mode = 'append')");
            }
            session.execute(ddl.icebergNamespace());
            if (previousMode != null && !previousMode.equals(modeName)) {
                // Append and upsert tables have different layouts; switching modes rebuilds the table.
                session.execute("DROP TABLE IF EXISTS " + ddl.icebergTableName(table));
            }
            boolean upsert = mode == TableMode.UPSERT;
            session.execute(ddl.icebergTable(table, source.columns(), upsert ? source.primaryKey() : List.of()));
            String from = ddl.topicTable(topic);
            if (!upsert && !source.primaryKey().isEmpty()) {
                // A keyed topic is an upsert changelog in the catalog; read every record instead.
                session.execute(ddl.appendLog(LOG, topic, source.primaryKey(), jobName));
                from = FlinkDdl.quote(LOG);
            }
            String columns = FlinkDdl.quoteAll(source.columns().stream().map(FlinkGateway.Column::name).toList());
            String jobId = session.execute("INSERT INTO " + ddl.icebergTableName(table) + " SELECT " + columns + " FROM " + from).jobId();
            return Outcome.pending("submitted job " + jobId, details);
        });
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

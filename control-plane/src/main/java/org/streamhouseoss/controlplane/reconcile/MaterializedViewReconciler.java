package org.streamhouseoss.controlplane.reconcile;

import java.util.List;

import org.streamhouseoss.controlplane.clients.FlinkGateway;
import org.streamhouseoss.controlplane.flink.FlinkDdl;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * A materialized view is shorthand for a statement: {@code CREATE TABLE <view> (PRIMARY KEY ...)
 * WITH ('changelog.mode' = 'upsert') AS <query>}, run in the topic catalog. Dropping the view stops
 * its job and drops the table, which deletes its topic and schemas.
 */
@ApplicationScoped
public class MaterializedViewReconciler implements Reconciler {

    private final StatementReconciler statements;
    private final FlinkJobSupport jobs;
    private final FlinkGateway gateway;
    private final FlinkDdl ddl;

    public MaterializedViewReconciler(StatementReconciler statements, FlinkJobSupport jobs, FlinkGateway gateway, FlinkDdl ddl) {
        this.statements = statements;
        this.jobs = jobs;
        this.gateway = gateway;
        this.ddl = ddl;
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
        return statements.runSql(stored, prefix(mv.name()), FlinkDdl.materializedView(mv.name(), mv.primaryKey(), mv.query()));
    }

    @Override
    public boolean delete(StoredResource stored, Topology topology) {
        jobs.cancelAll(prefix(stored.name()));
        try (FlinkGateway.Session session = gateway.open("drop-" + stored.name())) {
            session.execute("DROP TABLE IF EXISTS " + ddl.topicTable(stored.name()));
        }
        return true;
    }

    @Override
    public List<Edge> lineage(Resource resource, Topology topology) {
        return topology.lineage(resource);
    }
}

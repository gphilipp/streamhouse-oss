package org.streamhouseoss.controlplane.reconcile;

import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.ContextEngineClient;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

import jakarta.enterprise.context.ApplicationScoped;

/** Grants on context tables are enforced by the context engine for REST and MCP queries. */
@ApplicationScoped
public class GrantReconciler implements Reconciler {

    private final ContextEngineClient engine;

    public GrantReconciler(ContextEngineClient engine) {
        this.engine = engine;
    }

    @Override
    public ResourceKind kind() {
        return ResourceKind.GRANT;
    }

    @Override
    public Outcome reconcile(StoredResource stored, Topology topology) {
        Resource.Grant grant = (Resource.Grant) stored.resource();
        try {
            engine.grant(grant.objectName(), grant.role());
        } catch (ComponentException e) {
            return Outcome.failed(e.getMessage());
        }
        return Outcome.ready("role " + grant.role() + " may " + grant.privilege() + " context table " + grant.objectName());
    }

    @Override
    public boolean delete(StoredResource stored, Topology topology) {
        Resource.Grant grant = (Resource.Grant) stored.resource();
        engine.revoke(grant.objectName(), grant.role());
        return true;
    }
}

package org.streamhouseoss.controlplane.reconcile;

import java.util.HashMap;
import java.util.Map;

import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.Secrets;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

import jakarta.enterprise.context.ApplicationScoped;

/** Connections hold no external state; reconciling checks that their options and secrets resolve. */
@ApplicationScoped
public class ConnectionReconciler implements Reconciler {

    private final Secrets secrets;

    public ConnectionReconciler(Secrets secrets) {
        this.secrets = secrets;
    }

    @Override
    public ResourceKind kind() {
        return ResourceKind.CONNECTION;
    }

    @Override
    public Outcome reconcile(StoredResource stored, Topology topology) {
        Resource.Connection connection = (Resource.Connection) stored.resource();
        try {
            Map<String, String> resolved = new HashMap<>();
            for (String option : Resource.Connection.REQUIRED_OPTIONS) {
                resolved.put(option, secrets.resolve(connection.options(), option));
            }
            return Outcome.ready(connection.type().name().toLowerCase() + " connection to "
                    + resolved.get("host") + "/" + resolved.get("database"));
        } catch (ComponentException e) {
            return Outcome.failed(e.getMessage());
        }
    }

    @Override
    public boolean delete(StoredResource resource, Topology topology) {
        return true;
    }
}

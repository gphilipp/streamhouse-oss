package org.streamhouseoss.controlplane.reconcile;

import java.util.List;

import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.Secrets;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

import jakarta.enterprise.context.ApplicationScoped;

/** Connections hold no external state; reconciling checks that their options and secrets resolve. */
@ApplicationScoped
public class ConnectionReconciler implements Reconciler {

    static final List<String> REQUIRED = List.of("host", "database", "user", "password");

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
            for (String option : REQUIRED) {
                secrets.resolve(connection.options(), option);
            }
        } catch (ComponentException e) {
            return Outcome.failed(e.getMessage());
        }
        return Outcome.ready(connection.type().name().toLowerCase() + " connection to "
                + secrets.resolve(connection.options(), "host") + "/" + secrets.resolve(connection.options(), "database"));
    }

    @Override
    public boolean delete(StoredResource resource, Topology topology) {
        return true;
    }
}

package org.streamhouseoss.controlplane.reconcile;

import java.util.List;
import java.util.Map;

import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.Phase;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

/** Drives one kind of resource towards its declared state. Implementations must be idempotent. */
public interface Reconciler {

    record Outcome(Phase phase, String message, Map<String, Object> details) {
        public static Outcome ready(String message) {
            return new Outcome(Phase.READY, message, Map.of());
        }

        public static Outcome ready(String message, Map<String, Object> details) {
            return new Outcome(Phase.READY, message, details);
        }

        public static Outcome pending(String message) {
            return new Outcome(Phase.PENDING, message, Map.of());
        }

        public static Outcome pending(String message, Map<String, Object> details) {
            return new Outcome(Phase.PENDING, message, details);
        }

        public static Outcome failed(String message) {
            return new Outcome(Phase.FAILED, message, Map.of());
        }
    }

    ResourceKind kind();

    Outcome reconcile(StoredResource resource, Topology topology);

    /** Removes external state; returns true once nothing is left. */
    boolean delete(StoredResource resource, Topology topology);

    /** Dataset edges this resource creates, for lineage. */
    default List<Edge> lineage(Resource resource, Topology topology) {
        return List.of();
    }
}

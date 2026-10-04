package org.streamhouseoss.controlplane.state;

import java.time.OffsetDateTime;
import java.util.Map;

import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

/** A resource with its bookkeeping and last observed status. */
public record StoredResource(
        Resource resource,
        String statement,
        long generation,
        boolean deleted,
        Phase phase,
        String message,
        long observedGeneration,
        Map<String, Object> details,
        OffsetDateTime statusUpdatedAt) {

    public ResourceKind kind() {
        return resource.kind();
    }

    public String name() {
        return resource.name();
    }

    /** True when the reconcilers have caught up with the latest change. */
    public boolean settled() {
        return observedGeneration >= generation && (phase == Phase.READY || phase == Phase.FAILED);
    }
}

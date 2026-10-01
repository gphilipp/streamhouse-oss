package org.streamhouseoss.controlplane.reconcile;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.ContextEngineClient;
import org.streamhouseoss.controlplane.clients.KafkaTopics;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.enterprise.context.ApplicationScoped;

/** Context tables are enabled on the context engine, which materializes the topic for lightning queries. */
@ApplicationScoped
public class ContextTableReconciler implements Reconciler {

    private final ContextEngineClient engine;
    private final KafkaTopics topics;

    public ContextTableReconciler(ContextEngineClient engine, KafkaTopics topics) {
        this.engine = engine;
        this.topics = topics;
    }

    @Override
    public ResourceKind kind() {
        return ResourceKind.CONTEXT_TABLE;
    }

    @Override
    public Outcome reconcile(StoredResource stored, Topology topology) {
        Resource.ContextTable table = (Resource.ContextTable) stored.resource();
        if (!topics.exists(table.name())) {
            return Outcome.pending("waiting for topic " + table.name());
        }
        try {
            Optional<JsonNode> current = engine.table(table.name());
            JsonNode info = current.isEmpty() || stored.observedGeneration() < stored.generation()
                    ? engine.enable(table.name(), table.mode(), table.description())
                    : current.get();
            String status = info.path("status").asText();
            Map<String, Object> details = Map.of("mode", info.path("mode").asText().toLowerCase(), "engineStatus", status);
            return switch (status) {
                case "ACTIVE" -> Outcome.ready("materialized; latest record at " + info.path("lastRecordTimestamp").asText("n/a"), details);
                case "WAITING_FOR_DATA" -> Outcome.ready("enabled; waiting for the first record", details);
                default -> Outcome.failed("context engine: " + info.path("statusMessage").asText(status));
            };
        } catch (ComponentException e) {
            return Outcome.failed(e.getMessage());
        }
    }

    @Override
    public boolean delete(StoredResource stored, Topology topology) {
        engine.disable(stored.name());
        return true;
    }

    @Override
    public List<Edge> lineage(Resource resource, Topology topology) {
        return List.of(new Edge(Edge.kafka(resource.name()), Edge.context(resource.name()), ResourceKind.CONTEXT_TABLE, resource.name()));
    }
}

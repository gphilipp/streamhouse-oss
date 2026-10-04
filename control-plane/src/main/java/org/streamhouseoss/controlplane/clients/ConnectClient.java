package org.streamhouseoss.controlplane.clients;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.enterprise.context.ApplicationScoped;

/** Connector lifecycle on Kafka Connect. */
@ApplicationScoped
public class ConnectClient {

    /** Connector state plus the first failed task's trace, if any. */
    public record ConnectorStatus(String connectorState, String failedTaskTrace, int tasks) {
        public boolean running() {
            return "RUNNING".equals(connectorState) && failedTaskTrace == null && tasks > 0;
        }
    }

    private final ConnectApi api;

    public ConnectClient(@RestClient ConnectApi api) {
        this.api = api;
    }

    /** Creates or updates a connector (idempotent). */
    public void put(String name, Map<String, String> config) {
        Http.ok(api.putConfig(name, config), "configuring connector " + name);
    }

    public Optional<ConnectorStatus> status(String name) {
        RestResponse<JsonNode> response = api.status(name);
        if (Http.notFound(response)) {
            return Optional.empty();
        }
        JsonNode body = Http.ok(response, "reading status of connector " + name).getEntity();
        String failedTrace = null;
        for (JsonNode task : body.path("tasks")) {
            if ("FAILED".equals(task.path("state").asText())) {
                failedTrace = task.path("trace").asText("task failed");
                break;
            }
        }
        if (failedTrace == null && "FAILED".equals(body.path("connector").path("state").asText())) {
            failedTrace = body.path("connector").path("trace").asText("connector failed");
        }
        return Optional.of(new ConnectorStatus(body.path("connector").path("state").asText(), failedTrace, body.path("tasks").size()));
    }

    public void restartFailed(String name) {
        api.restart(name, true, true);
    }

    /**
     * Deletes a connector and its committed offsets. Without the offset reset, a connector created
     * later under the same name would resume from the old position and skip its initial snapshot.
     */
    public void delete(String name) {
        if (status(name).isEmpty()) {
            return;
        }
        Http.ok(api.stop(name), "stopping connector " + name);
        Poll.until(() -> status(name).map(s -> "STOPPED".equals(s.connectorState())).orElse(true),
                Duration.ofSeconds(30), Duration.ofMillis(500), "connector " + name + " to stop");
        Http.ok(api.resetOffsets(name), "resetting offsets of connector " + name);
        RestResponse<Void> deleted = api.delete(name);
        if (!Http.notFound(deleted)) {
            Http.ok(deleted, "deleting connector " + name);
        }
    }
}

package org.streamhouseoss.controlplane.clients;

import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.StreamhouseConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

/** Kafka Connect REST API. */
@ApplicationScoped
public class ConnectClient {

    /** Connector state plus the first failed task's trace, if any. */
    public record ConnectorStatus(String connectorState, String failedTaskTrace, int tasks) {
        public boolean running() {
            return "RUNNING".equals(connectorState) && failedTaskTrace == null && tasks > 0;
        }
    }

    private final JsonHttp http;

    public ConnectClient(StreamhouseConfig config, ObjectMapper mapper) {
        this.http = new JsonHttp(mapper, config.connectUrl(), Map.of());
    }

    /** Creates or updates a connector (idempotent). */
    public void put(String name, Map<String, String> config) {
        http.put("/connectors/" + name + "/config", config).requireOk("configuring connector " + name);
    }

    public Optional<ConnectorStatus> status(String name) {
        JsonHttp.Response response = http.get("/connectors/" + name + "/status");
        if (response.status() == 404) {
            return Optional.empty();
        }
        JsonNode body = response.requireOk("reading status of connector " + name).body();
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
        http.post("/connectors/" + name + "/restart?includeTasks=true&onlyFailed=true", null);
    }

    /**
     * Deletes a connector and its committed offsets. Without the offset reset, a connector created
     * later under the same name would resume from the old position and skip its initial snapshot.
     */
    public void delete(String name) {
        if (status(name).isEmpty()) {
            return;
        }
        http.put("/connectors/" + name + "/stop", null).requireOk("stopping connector " + name);
        java.time.Instant deadline = java.time.Instant.now().plusSeconds(30);
        while (!"STOPPED".equals(status(name).map(ConnectorStatus::connectorState).orElse("STOPPED"))) {
            if (java.time.Instant.now().isAfter(deadline)) {
                throw new ComponentException("connector " + name + " did not stop");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ComponentException("interrupted", e);
            }
        }
        http.delete("/connectors/" + name + "/offsets").requireOk("resetting offsets of connector " + name);
        JsonHttp.Response response = http.delete("/connectors/" + name);
        if (response.status() != 404) {
            response.requireOk("deleting connector " + name);
        }
    }
}

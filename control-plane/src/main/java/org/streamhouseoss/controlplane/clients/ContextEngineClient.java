package org.streamhouseoss.controlplane.clients;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.resteasy.reactive.RestResponse;
import org.streamhouseoss.model.TableMode;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.enterprise.context.ApplicationScoped;

/** Context tables and grants on the context engine. */
@ApplicationScoped
public class ContextEngineClient {

    private final ContextEngineApi api;

    public ContextEngineClient(@RestClient ContextEngineApi api) {
        this.api = api;
    }

    /** Enables a topic; a null mode lets the engine infer it from the topic's cleanup.policy. */
    public JsonNode enable(String topic, TableMode mode, String description) {
        Map<String, Object> request = new HashMap<>();
        request.put("mode", mode);
        request.put("description", description);
        return Http.ok(api.enable(topic, request), "enabling context table " + topic).getEntity();
    }

    public Optional<JsonNode> table(String topic) {
        RestResponse<JsonNode> response = api.table(topic);
        return Http.notFound(response) ? Optional.empty() : Optional.of(Http.ok(response, "reading context table " + topic).getEntity());
    }

    public void disable(String topic) {
        RestResponse<Void> response = api.disable(topic);
        if (!Http.notFound(response)) {
            Http.ok(response, "disabling context table " + topic);
        }
    }

    public void grant(String topic, String role) {
        Http.ok(api.grant(topic, role), "granting " + role + " on " + topic);
    }

    public void revoke(String topic, String role) {
        Http.ok(api.revoke(topic, role), "revoking " + role + " on " + topic);
    }
}

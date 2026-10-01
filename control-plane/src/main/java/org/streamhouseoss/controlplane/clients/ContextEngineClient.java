package org.streamhouseoss.controlplane.clients;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.StreamhouseConfig;
import org.streamhouseoss.model.TableMode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.Tokens;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;

/** The context engine's admin API, called with the control plane's own service-account token. */
@ApplicationScoped
public class ContextEngineClient {

    private final JsonHttp http;
    private final Instance<OidcClient> oidc;
    private Tokens tokens;

    public ContextEngineClient(StreamhouseConfig config, ObjectMapper mapper, Instance<OidcClient> oidc) {
        this.http = new JsonHttp(mapper, config.contextEngineUrl(), Map.of());
        this.oidc = oidc;
    }

    /** Enables a topic; a null mode lets the engine infer it from the topic's cleanup.policy. */
    public JsonNode enable(String topic, TableMode mode, String description) {
        Map<String, Object> body = new HashMap<>();
        body.put("mode", mode);
        body.put("description", description);
        return call("PUT", "/admin/v1/tables/" + topic, body).requireOk("enabling context table " + topic).body();
    }

    public Optional<JsonNode> table(String topic) {
        JsonHttp.Response response = call("GET", "/admin/v1/tables/" + topic, null);
        return response.status() == 404 ? Optional.empty() : Optional.of(response.requireOk("reading context table " + topic).body());
    }

    public void disable(String topic) {
        JsonHttp.Response response = call("DELETE", "/admin/v1/tables/" + topic, null);
        if (response.status() != 404) {
            response.requireOk("disabling context table " + topic);
        }
    }

    public void grant(String topic, String role) {
        call("PUT", "/admin/v1/grants/" + topic + "/" + role, null).requireOk("granting " + role + " on " + topic);
    }

    public void revoke(String topic, String role) {
        call("DELETE", "/admin/v1/grants/" + topic + "/" + role, null).requireOk("revoking " + role + " on " + topic);
    }

    private JsonHttp.Response call(String method, String path, Object body) {
        return http.send(method, path, body, Map.of("Authorization", "Bearer " + accessToken()));
    }

    private synchronized String accessToken() {
        if (tokens == null || tokens.isAccessTokenWithinRefreshInterval() || tokens.isAccessTokenExpired()) {
            OidcClient client = oidc.get();
            tokens = tokens != null && tokens.getRefreshToken() != null && !tokens.isRefreshTokenExpired()
                    ? client.refreshTokens(tokens.getRefreshToken()).await().atMost(Duration.ofSeconds(10))
                    : client.getTokens().await().atMost(Duration.ofSeconds(10));
        }
        return tokens.getAccessToken();
    }
}

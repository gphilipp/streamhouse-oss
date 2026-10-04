package org.streamhouseoss.cli;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.OidcClientConfigBuilder;
import io.quarkus.oidc.client.OidcClients;
import io.quarkus.oidc.client.Tokens;
import io.quarkus.oidc.client.runtime.OidcClientConfig;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;

/**
 * Tokens and API calls. Tokens come from Keycloak through quarkus-oidc-client (password, device
 * code, refresh) and are kept in {@code ~/.streamhouse/token.json}, readable by the user only;
 * {@code SHCTL_TOKEN} overrides them. API calls go through typed REST clients.
 */
final class Session {

    static final String CLIENT_ID = "shctl";
    private static final Path TOKEN_FILE = Path.of(System.getProperty("user.home"), ".streamhouse", "token.json");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Endpoints endpoints;
    private final OidcClients oidc;

    Session(Endpoints endpoints, OidcClients oidc) {
        this.endpoints = endpoints;
        this.oidc = oidc;
    }

    // ---- tokens -------------------------------------------------------------------------------

    /** An OIDC client for the shctl public client with the given grant. */
    OidcClient client(OidcClientConfig.Grant.Type grant, Map<String, String> grantOptions) {
        OidcClientConfigBuilder config = OidcClientConfig.builder()
                .id(CLIENT_ID + "-" + grant.name().toLowerCase())
                .authServerUrl(endpoints.issuer)
                .clientId(CLIENT_ID)
                .grant(grant);
        if (!grantOptions.isEmpty()) {
            config.grantOptions(grant.name().toLowerCase(), grantOptions);
        }
        try {
            return oidc.newClient(config.build()).await().atMost(TIMEOUT);
        } catch (RuntimeException e) {
            throw new CliException("cannot reach " + endpoints.issuer + " (" + e.getMessage() + ")");
        }
    }

    Tokens await(io.smallrye.mutiny.Uni<Tokens> tokens) {
        return tokens.await().atMost(TIMEOUT);
    }

    /** The device authorization request (RFC 8628 step 1): user code, verification URL, device code. */
    JsonNode authorizeDevice() {
        DeviceAuthorizationApi api = QuarkusRestClientBuilder.newBuilder().baseUri(URI.create(endpoints.issuer))
                .build(DeviceAuthorizationApi.class);
        return call(endpoints.issuer, () -> api.authorize(CLIENT_ID, "openid"));
    }

    void saveTokens(Tokens tokens) {
        ObjectNode saved = mapper.createObjectNode();
        saved.put("access_token", tokens.getAccessToken());
        saved.put("refresh_token", tokens.getRefreshToken());
        Long expiresAt = tokens.getAccessTokenExpiresAt();
        saved.put("expires_at", expiresAt == null ? Instant.now().plusSeconds(60).getEpochSecond() : expiresAt - 10);
        saved.put("issuer", endpoints.issuer);
        try {
            Files.createDirectories(TOKEN_FILE.getParent());
            Files.writeString(TOKEN_FILE, mapper.writeValueAsString(saved));
            try {
                Files.setPosixFilePermissions(TOKEN_FILE, PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException ignored) {
                // non-POSIX file system
            }
        } catch (IOException e) {
            throw new CliException("cannot save tokens to " + TOKEN_FILE + ": " + e.getMessage());
        }
    }

    /** A valid access token: from SHCTL_TOKEN, the saved login, or a refresh of it. */
    String accessToken() {
        String fromEnv = System.getenv("SHCTL_TOKEN");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        if (!Files.exists(TOKEN_FILE)) {
            throw new CliException("not logged in; run: shctl login");
        }
        try {
            JsonNode saved = mapper.readTree(Files.readString(TOKEN_FILE));
            if (Instant.now().getEpochSecond() < saved.path("expires_at").asLong()) {
                return saved.path("access_token").asText();
            }
            Tokens refreshed = await(client(OidcClientConfig.Grant.Type.REFRESH, Map.of())
                    .refreshTokens(saved.path("refresh_token").asText()));
            saveTokens(refreshed);
            return refreshed.getAccessToken();
        } catch (IOException | RuntimeException e) {
            throw new CliException("session expired; run: shctl login");
        }
    }

    // ---- API calls ----------------------------------------------------------------------------

    JsonNode sql(String sql) {
        ControlPlaneApi api = api(endpoints.server, ControlPlaneApi.class);
        return call(endpoints.server, () -> api.sql(bearer(), Map.of("sql", sql)));
    }

    JsonNode resources(String kind) {
        ControlPlaneApi api = api(endpoints.server, ControlPlaneApi.class);
        return call(endpoints.server, () -> api.resources(bearer(), kind));
    }

    JsonNode query(String sql) {
        ContextEngineApi api = api(endpoints.contextEngine, ContextEngineApi.class);
        return call(endpoints.contextEngine, () -> api.query(bearer(), Map.of("query", sql)));
    }

    private String bearer() {
        return "Bearer " + accessToken();
    }

    private static <T> T api(String baseUrl, Class<T> type) {
        return QuarkusRestClientBuilder.newBuilder().baseUri(URI.create(baseUrl)).build(type);
    }

    /** Runs a REST call and turns HTTP and connection errors into CLI messages. */
    private <T> T call(String target, Supplier<T> request) {
        try {
            return request.get();
        } catch (WebApplicationException e) {
            int status = e.getResponse().getStatus();
            JsonNode body = readBody(e);
            String message = body.has("error_description") ? body.get("error_description").asText()
                    : body.has("error") ? body.get("error").asText() : "HTTP " + status;
            if (status == 401) {
                message = "not authenticated (" + message + "); run: shctl login";
            } else if (status == 403) {
                message = "forbidden: " + message;
            } else if (body.has("line")) {
                message = "syntax error at " + message;
            }
            throw new CliException(message, body);
        } catch (ProcessingException e) {
            throw new CliException("cannot reach " + target + " (" + rootMessage(e) + ")");
        }
    }

    private JsonNode readBody(WebApplicationException e) {
        try {
            String text = e.getResponse().readEntity(String.class);
            return text == null || text.isBlank() ? mapper.createObjectNode() : mapper.readTree(text);
        } catch (RuntimeException | IOException notJson) {
            return mapper.createObjectNode();
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage() == null ? root.toString() : root.getMessage();
    }
}

package org.streamhouseoss.cli;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Tokens and HTTP calls. Tokens are kept in {@code ~/.streamhouse/token.json} (readable by the
 * user only) and refreshed with the refresh token when they expire.
 */
final class Session {

    static final String CLIENT_ID = "shctl";
    private static final Path TOKEN_FILE = Path.of(System.getProperty("user.home"), ".streamhouse", "token.json");

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final Endpoints endpoints;

    Session(Endpoints endpoints) {
        this.endpoints = endpoints;
    }

    ObjectMapper mapper() {
        return mapper;
    }

    // ---- tokens -------------------------------------------------------------------------------

    JsonNode tokenRequest(Map<String, String> form) {
        String body = form.entrySet().stream()
                .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoints.issuer + "/protocol/openid-connect/token"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return send(request);
    }

    JsonNode deviceAuthorization() {
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoints.issuer + "/protocol/openid-connect/auth/device"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("client_id=" + CLIENT_ID + "&scope=openid"))
                .build();
        return send(request);
    }

    void saveTokens(JsonNode tokens) {
        ObjectNode saved = tokens.deepCopy();
        saved.put("expires_at", Instant.now().plusSeconds(tokens.path("expires_in").asLong(60) - 10).getEpochSecond());
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
            JsonNode refreshed = tokenRequest(Map.of("grant_type", "refresh_token", "client_id", CLIENT_ID,
                    "refresh_token", saved.path("refresh_token").asText()));
            saveTokens(refreshed);
            return refreshed.path("access_token").asText();
        } catch (IOException | CliException e) {
            throw new CliException("session expired; run: shctl login");
        }
    }

    // ---- API calls ----------------------------------------------------------------------------

    JsonNode post(String baseUrl, String path, Object body) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .header("Authorization", "Bearer " + accessToken())
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(60))
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                    .build();
            return send(request);
        } catch (IOException e) {
            throw new CliException(e.getMessage());
        }
    }

    JsonNode get(String baseUrl, String path) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Authorization", "Bearer " + accessToken())
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
        return send(request);
    }

    private JsonNode send(HttpRequest request) {
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode body = response.body() == null || response.body().isBlank() ? mapper.createObjectNode() : mapper.readTree(response.body());
            if (response.statusCode() >= 400) {
                String message = body.has("error_description") ? body.get("error_description").asText()
                        : body.has("error") ? body.get("error").asText() : response.body();
                if (response.statusCode() == 401) {
                    message = "not authenticated (" + message + "); run: shctl login";
                } else if (response.statusCode() == 403) {
                    message = "forbidden: " + message;
                }
                throw new CliException(message, body);
            }
            return body;
        } catch (IOException e) {
            throw new CliException("cannot reach " + request.uri().getHost() + ":" + request.uri().getPort() + " (" + e.getMessage() + ")");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CliException("interrupted");
        }
    }
}

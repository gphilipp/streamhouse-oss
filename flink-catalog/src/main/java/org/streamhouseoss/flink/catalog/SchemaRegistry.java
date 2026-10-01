package org.streamhouseoss.flink.catalog;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Minimal client for a Confluent-compatible schema registry API (e.g. Apicurio's ccompat). */
class SchemaRegistry {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final String baseUrl;

    SchemaRegistry(String baseUrl) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
    }

    Set<String> subjects() {
        Set<String> subjects = new HashSet<>();
        send("GET", "/subjects", null).ifPresent(body -> body.forEach(s -> subjects.add(s.asText())));
        return subjects;
    }

    /** The latest schema text of a subject (AVRO only), if registered. */
    Optional<String> latest(String subject) {
        return send("GET", "/subjects/" + encode(subject) + "/versions/latest", null).map(body -> {
            String type = body.path("schemaType").asText("AVRO");
            if (!"AVRO".equals(type)) {
                throw new CatalogException("subject " + subject + " has a " + type + " schema; only Avro is supported");
            }
            return body.path("schema").asText();
        });
    }

    void register(String subject, String schema) {
        try {
            send("POST", "/subjects/" + encode(subject) + "/versions", JSON.writeValueAsString(Map.of("schema", schema)))
                    .orElseThrow(() -> new CatalogException("cannot register schema under " + subject));
        } catch (IOException e) {
            throw new CatalogException(e);
        }
    }

    void delete(String subject) {
        send("DELETE", "/subjects/" + encode(subject), null);
        send("DELETE", "/subjects/" + encode(subject) + "?permanent=true", null);
    }

    /** Sends a request; 404 means "absent". */
    private Optional<JsonNode> send(String method, String path, String body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/vnd.schemaregistry.v1+json, application/json");
        if (body == null) {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            request.header("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        try {
            HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 404) {
                return Optional.empty();
            }
            if (response.statusCode() >= 300) {
                throw new CatalogException("schema registry " + method + " " + path + " returned " + response.statusCode()
                        + ": " + response.body());
            }
            return Optional.of(response.body().isBlank() ? JSON.createObjectNode() : JSON.readTree(response.body()));
        } catch (IOException e) {
            throw new CatalogException("schema registry " + baseUrl + " unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CatalogException("interrupted", e);
        }
    }

    private static String encode(String subject) {
        return URLEncoder.encode(subject, StandardCharsets.UTF_8);
    }
}

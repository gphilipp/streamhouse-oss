package org.streamhouseoss.context.schema;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.streamhouseoss.context.ContextConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Minimal client for a Confluent-compatible schema registry (e.g. Apicurio's ccompat API).
 * Schemas are immutable per id, so they are cached forever.
 */
@ApplicationScoped
public class SchemaRegistryClient {

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Map<Integer, Schema> byId = new ConcurrentHashMap<>();
    private final ObjectMapper mapper;
    private final String baseUrl;

    public SchemaRegistryClient(ContextConfig config, ObjectMapper mapper) {
        this.mapper = mapper;
        this.baseUrl = config.registryUrl().replaceAll("/+$", "");
    }

    public Schema schemaById(int id) {
        return byId.computeIfAbsent(id, this::fetch);
    }

    private Schema fetch(int id) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/schemas/ids/" + id))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new SchemaException("schema registry returned " + response.statusCode() + " for schema id " + id);
            }
            JsonNode body = mapper.readTree(response.body());
            String schemaType = body.path("schemaType").asText("AVRO");
            if (!"AVRO".equals(schemaType)) {
                throw new SchemaException("schema id " + id + " is " + schemaType + "; only AVRO is supported");
            }
            return new Schema.Parser().parse(body.get("schema").asText());
        } catch (IOException e) {
            throw new SchemaException("cannot fetch schema id " + id + " from " + baseUrl + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaException("interrupted while fetching schema id " + id, e);
        }
    }
}

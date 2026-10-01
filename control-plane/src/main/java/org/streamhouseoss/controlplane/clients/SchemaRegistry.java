package org.streamhouseoss.controlplane.clients;

import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.streamhouseoss.controlplane.StreamhouseConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

/** Confluent-compatible schema registry API (Apicurio ccompat). */
@ApplicationScoped
public class SchemaRegistry {

    private final JsonHttp http;

    public SchemaRegistry(StreamhouseConfig config, ObjectMapper mapper) {
        this.http = new JsonHttp(mapper, config.registryUrl(), Map.of());
    }

    /** The latest Avro schema registered for a subject, if any. */
    public Optional<Schema> latest(String subject) {
        JsonHttp.Response response = http.get("/subjects/" + subject + "/versions/latest");
        if (response.status() == 404) {
            return Optional.empty();
        }
        String schema = response.requireOk("reading schema " + subject).body().path("schema").asText();
        return Optional.of(new Schema.Parser().parse(schema));
    }

    public void deleteSubject(String subject) {
        http.delete("/subjects/" + subject);
        http.delete("/subjects/" + subject + "?permanent=true");
    }
}

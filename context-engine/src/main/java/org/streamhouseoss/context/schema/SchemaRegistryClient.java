package org.streamhouseoss.context.schema;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.streamhouseoss.context.ContextConfig;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;

/**
 * Reads schemas by id from a Confluent-compatible schema registry (e.g. Apicurio's ccompat API).
 * Schemas are immutable per id, so they are cached forever.
 */
@ApplicationScoped
public class SchemaRegistryClient {

    /** The one registry call the context engine needs. */
    @Path("/schemas/ids")
    @Produces({ "application/vnd.schemaregistry.v1+json", "application/json" })
    public interface Api {
        @GET
        @Path("/{id}")
        RegisteredSchema schema(@PathParam("id") int id);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RegisteredSchema(String schema, String schemaType) {
    }

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final Map<Integer, Schema> byId = new ConcurrentHashMap<>();
    private final Api api;
    private final String baseUrl;

    public SchemaRegistryClient(ContextConfig config) {
        this.baseUrl = config.registryUrl().replaceAll("/+$", "");
        this.api = QuarkusRestClientBuilder.newBuilder()
                .baseUri(URI.create(baseUrl))
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                .build(Api.class);
    }

    public Schema schemaById(int id) {
        return byId.computeIfAbsent(id, this::fetch);
    }

    private Schema fetch(int id) {
        RegisteredSchema registered;
        try {
            registered = api.schema(id);
        } catch (WebApplicationException e) {
            throw new SchemaException("schema registry returned " + e.getResponse().getStatus() + " for schema id " + id);
        } catch (ProcessingException e) {
            throw new SchemaException("cannot fetch schema id " + id + " from " + baseUrl + ": " + e.getMessage(), e);
        }
        String type = registered.schemaType() == null ? "AVRO" : registered.schemaType();
        if (!"AVRO".equals(type)) {
            throw new SchemaException("schema id " + id + " is " + type + "; only AVRO is supported");
        }
        return new Schema.Parser().parse(registered.schema());
    }
}

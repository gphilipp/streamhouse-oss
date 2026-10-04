package org.streamhouseoss.controlplane.clients;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

/** Confluent-compatible schema registry API (Apicurio ccompat). */
@RegisterRestClient(configKey = "schema-registry")
@Path("/subjects/{subject}")
public interface SchemaRegistryApi {

    @DELETE
    RestResponse<JsonNode> delete(@PathParam("subject") String subject, @QueryParam("permanent") boolean permanent);
}

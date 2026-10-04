package org.streamhouseoss.controlplane.clients;

import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.JsonNode;

import io.quarkus.oidc.client.filter.OidcClientFilter;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;

/** The context engine's admin API, called with the control plane's own service-account token. */
@RegisterRestClient(configKey = "context-engine")
@OidcClientFilter
@Path("/admin/v1")
public interface ContextEngineApi {

    @PUT
    @Path("/tables/{topic}")
    RestResponse<JsonNode> enable(@PathParam("topic") String topic, Map<String, Object> request);

    @GET
    @Path("/tables/{topic}")
    RestResponse<JsonNode> table(@PathParam("topic") String topic);

    @DELETE
    @Path("/tables/{topic}")
    RestResponse<Void> disable(@PathParam("topic") String topic);

    @PUT
    @Path("/grants/{topic}/{role}")
    RestResponse<Void> grant(@PathParam("topic") String topic, @PathParam("role") String role);

    @DELETE
    @Path("/grants/{topic}/{role}")
    RestResponse<Void> revoke(@PathParam("topic") String topic, @PathParam("role") String role);
}

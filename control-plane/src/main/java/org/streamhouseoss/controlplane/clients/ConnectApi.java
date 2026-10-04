package org.streamhouseoss.controlplane.clients;

import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

/** Kafka Connect REST API. */
@RegisterRestClient(configKey = "connect")
@Path("/connectors/{name}")
public interface ConnectApi {

    @PUT
    @Path("/config")
    RestResponse<JsonNode> putConfig(@PathParam("name") String name, Map<String, String> config);

    @GET
    @Path("/status")
    RestResponse<JsonNode> status(@PathParam("name") String name);

    @POST
    @Path("/restart")
    RestResponse<Void> restart(@PathParam("name") String name, @QueryParam("includeTasks") boolean includeTasks,
            @QueryParam("onlyFailed") boolean onlyFailed);

    @PUT
    @Path("/stop")
    RestResponse<Void> stop(@PathParam("name") String name);

    @DELETE
    @Path("/offsets")
    RestResponse<JsonNode> resetOffsets(@PathParam("name") String name);

    @DELETE
    RestResponse<Void> delete(@PathParam("name") String name);
}

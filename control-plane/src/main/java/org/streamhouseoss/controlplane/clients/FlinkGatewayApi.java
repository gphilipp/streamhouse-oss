package org.streamhouseoss.controlplane.clients;

import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

/** Flink SQL Gateway REST API (v3). */
@RegisterRestClient(configKey = "flink-gateway")
@Path("/v3/sessions")
public interface FlinkGatewayApi {

    @POST
    RestResponse<JsonNode> openSession(Map<String, Object> request);

    @POST
    @Path("/{session}/statements")
    RestResponse<JsonNode> execute(@PathParam("session") String session, Map<String, Object> request);

    @GET
    @Path("/{session}/operations/{operation}/result/{token}")
    RestResponse<JsonNode> result(@PathParam("session") String session, @PathParam("operation") String operation,
            @PathParam("token") long token, @QueryParam("rowFormat") String rowFormat);

    @DELETE
    @Path("/{session}/operations/{operation}/close")
    RestResponse<Void> closeOperation(@PathParam("session") String session, @PathParam("operation") String operation);

    @DELETE
    @Path("/{session}")
    RestResponse<Void> closeSession(@PathParam("session") String session);
}

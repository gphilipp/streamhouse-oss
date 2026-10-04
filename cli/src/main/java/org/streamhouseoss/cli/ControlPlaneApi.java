package org.streamhouseoss.cli;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/** The control plane API (built with QuarkusRestClientBuilder against --server). */
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface ControlPlaneApi {

    @POST
    @Path("/sql")
    JsonNode sql(@HeaderParam("Authorization") String authorization, Map<String, String> body);

    @GET
    @Path("/resources")
    JsonNode resources(@HeaderParam("Authorization") String authorization, @QueryParam("kind") String kind);
}

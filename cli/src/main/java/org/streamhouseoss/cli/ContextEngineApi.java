package org.streamhouseoss.cli;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** The context engine's lightning-query API (built against --context-engine). */
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface ContextEngineApi {

    @POST
    @Path("/query")
    JsonNode query(@HeaderParam("Authorization") String authorization, Map<String, String> body);
}

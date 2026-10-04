package org.streamhouseoss.controlplane.clients;

import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** Gravitino REST API. */
@RegisterRestClient(configKey = "gravitino")
@Path("/api")
@Produces("application/vnd.gravitino.v1+json")
@Consumes(MediaType.APPLICATION_JSON)
public interface GravitinoApi {

    @POST
    @Path("/metalakes")
    RestResponse<JsonNode> createMetalake(Map<String, Object> metalake);

    @POST
    @Path("/metalakes/{metalake}/catalogs")
    RestResponse<JsonNode> createCatalog(@PathParam("metalake") String metalake, Map<String, Object> catalog);

    /** OpenLineage RunEvent ingestion. */
    @POST
    @Path("/lineage")
    RestResponse<JsonNode> lineage(Map<String, Object> runEvent);
}

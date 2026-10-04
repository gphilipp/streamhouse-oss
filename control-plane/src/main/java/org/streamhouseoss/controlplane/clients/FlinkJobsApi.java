package org.streamhouseoss.controlplane.clients;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

/** Flink JobManager REST API. */
@RegisterRestClient(configKey = "flink-jobmanager")
@Path("/jobs")
public interface FlinkJobsApi {

    @GET
    @Path("/overview")
    RestResponse<JsonNode> overview();

    @PATCH
    @Path("/{job}")
    RestResponse<Void> cancel(@PathParam("job") String job, @QueryParam("mode") String mode);

    @GET
    @Path("/{job}/exceptions")
    RestResponse<JsonNode> exceptions(@PathParam("job") String job, @QueryParam("maxExceptions") int maxExceptions);
}

package org.streamhouseoss.context.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.resteasy.reactive.RestResponse;
import org.streamhouseoss.context.ingest.MaterializerManager;
import org.streamhouseoss.context.store.ServingStore;
import org.streamhouseoss.context.store.TableInfo;
import org.streamhouseoss.context.store.TableMode;

import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Admin API used by the control plane to reconcile context tables and grants. Idempotent: PUT
 * enables or updates, DELETE disables.
 */
@Path("/admin/v1")
@RolesAllowed("admin")
@Blocking
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AdminResource {

    /** {@code mode} may be null to infer it from the topic's cleanup.policy. */
    public record EnableRequest(TableMode mode, String description) {
    }

    private final MaterializerManager materializers;
    private final ServingStore store;

    public AdminResource(MaterializerManager materializers, ServingStore store) {
        this.materializers = materializers;
        this.store = store;
    }

    @GET
    @Path("/tables")
    public List<TableInfo> tables() {
        return store.tables();
    }

    @GET
    @Path("/tables/{topic}")
    public RestResponse<TableInfo> table(@PathParam("topic") String topic) {
        return store.table(topic).map(RestResponse::ok).orElse(RestResponse.notFound());
    }

    @PUT
    @Path("/tables/{topic}")
    public TableInfo enable(@PathParam("topic") String topic, EnableRequest request) {
        EnableRequest r = request == null ? new EnableRequest(null, null) : request;
        return materializers.enable(topic, r.mode(), r.description());
    }

    @DELETE
    @Path("/tables/{topic}")
    public RestResponse<Void> disable(@PathParam("topic") String topic) {
        return materializers.disable(topic) ? RestResponse.noContent() : RestResponse.notFound();
    }

    @GET
    @Path("/grants")
    public Map<String, Set<String>> grants() {
        return store.grants();
    }

    @PUT
    @Path("/grants/{topic}/{role}")
    public RestResponse<Void> grant(@PathParam("topic") String topic, @PathParam("role") String role) {
        store.grant(topic, role);
        return RestResponse.noContent();
    }

    @DELETE
    @Path("/grants/{topic}/{role}")
    public RestResponse<Void> revoke(@PathParam("topic") String topic, @PathParam("role") String role) {
        store.revoke(topic, role);
        return RestResponse.noContent();
    }
}

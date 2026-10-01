package org.streamhouseoss.controlplane.api;

import java.util.List;
import java.util.Map;

import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.streamhouseoss.controlplane.state.DesiredState;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.sql.SqlParseException;

import io.quarkus.security.Authenticated;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Blocking;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/** The control plane API: run SQL, list resources with their status. */
@Path("/v1")
@Authenticated
@Blocking
@Produces(MediaType.APPLICATION_JSON)
public class SqlResource {

    public record SqlRequest(String sql) {
    }

    /** A resource as shown to clients: identity, status and the statement that declared it. */
    public record ResourceView(String kind, String name, String phase, String message, long generation,
            long observedGeneration, boolean settled, String statement, Map<String, Object> details) {
        static ResourceView of(StoredResource r) {
            return new ResourceView(r.kind().name(), SqlService.display(r.resource()), r.phase().name(), r.message(),
                    r.generation(), r.observedGeneration(), r.settled(), r.statement(), r.details());
        }
    }

    private final SqlService sql;
    private final DesiredState state;
    private final SecurityIdentity identity;

    public SqlResource(SqlService sql, DesiredState state, SecurityIdentity identity) {
        this.sql = sql;
        this.state = state;
        this.identity = identity;
    }

    @POST
    @Path("/sql")
    @Consumes(MediaType.APPLICATION_JSON)
    public SqlService.ScriptResult execute(SqlRequest request) {
        if (request == null || request.sql() == null || request.sql().isBlank()) {
            throw new BadRequestException("body must be {\"sql\": \"...\"}");
        }
        return sql.execute(request.sql(), identity);
    }

    @GET
    @Path("/resources")
    public List<ResourceView> resources(@QueryParam("kind") String kind) {
        ResourceKind k = kind == null || kind.isBlank() ? null : ResourceKind.valueOf(kind.toUpperCase());
        return state.list(k).stream().map(ResourceView::of).toList();
    }

    @ServerExceptionMapper
    public RestResponse<Map<String, Object>> parseError(SqlParseException e) {
        return RestResponse.status(RestResponse.Status.BAD_REQUEST, Map.of("error", e.getMessage(), "line", e.line()));
    }

    @ServerExceptionMapper
    public RestResponse<Map<String, Object>> badKind(IllegalArgumentException e) {
        return RestResponse.status(RestResponse.Status.BAD_REQUEST, Map.of("error", String.valueOf(e.getMessage())));
    }
}

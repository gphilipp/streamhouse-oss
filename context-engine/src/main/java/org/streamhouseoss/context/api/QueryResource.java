package org.streamhouseoss.context.api;

import java.util.List;

import org.streamhouseoss.context.query.QueryService;
import org.streamhouseoss.context.security.Access;
import org.streamhouseoss.context.security.AuditLog;

import io.quarkus.security.Authenticated;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Blocking;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** Lightning queries and table discovery for applications. */
@Path("/v1")
@Authenticated
@Blocking
@Produces(MediaType.APPLICATION_JSON)
public class QueryResource {

    public record QueryRequest(String query) {
    }

    private final QueryService queries;
    private final SecurityIdentity identity;

    public QueryResource(QueryService queries, SecurityIdentity identity) {
        this.queries = queries;
        this.identity = identity;
    }

    @POST
    @Path("/query")
    @Consumes(MediaType.APPLICATION_JSON)
    public QueryService.QueryResult query(QueryRequest request) {
        if (request == null || request.query() == null || request.query().isBlank()) {
            throw new jakarta.ws.rs.BadRequestException("body must be {\"query\": \"SELECT ...\"}");
        }
        return queries.query(Access.Caller.of(identity), AuditLog.Channel.REST, request.query());
    }

    @GET
    @Path("/tables")
    public List<QueryService.TableSummary> tables() {
        return queries.listTables(Access.Caller.of(identity));
    }

    @GET
    @Path("/tables/{name}")
    public QueryService.TableMetadata table(@PathParam("name") String name) {
        return queries.metadata(Access.Caller.of(identity), name);
    }
}

package org.streamhouseoss.context.api;

import org.streamhouseoss.context.query.QueryException;
import org.streamhouseoss.context.query.QueryService;
import org.streamhouseoss.context.security.Access;
import org.streamhouseoss.context.security.AuditLog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolCallException;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.inject.Inject;

/**
 * MCP tools for AI agents, named like Confluent's Real-Time Context Engine so agents and prompts
 * written for it work unchanged. Results are JSON text; errors are returned as tool errors with
 * a message the agent can act on (fix the query, pick another table).
 */
public class ContextTools {

    private static final String QUERY_HELP = """
            Run a lightning query: a single-table SELECT answered from a continuously updated \
            materialization of a Kafka topic, in milliseconds. Supported: SELECT * | columns | \
            COUNT(*)/SUM/AVG/MIN/MAX(col); FROM <table>; WHERE with =, <>, <, <=, >, >=, BETWEEN, IN, \
            LIKE, IS NULL combined with AND/OR/NOT; ORDER BY; LIMIT (default 100, max 1000); OFFSET. \
            Not supported: joins, subqueries, GROUP BY, functions in WHERE. Dotted table names like \
            shop.public.orders can be written as-is. Filter on key columns for point lookups. \
            Call getMetadata first to learn the columns.""";

    @Inject
    QueryService queries;

    @Inject
    SecurityIdentity identity;

    @Inject
    ObjectMapper mapper;

    @Tool(name = "listTopics", description = "List the real-time tables (one per Kafka topic) you can query, "
            + "with their description, mode (upsert = latest state per key, append = every event) and freshness.",
            annotations = @Tool.Annotations(readOnlyHint = true, openWorldHint = false))
    String listTopics() {
        return json(queries.listTables(Access.Caller.of(identity)));
    }

    @Tool(name = "getMetadata", description = "Describe a real-time table: columns and types, key columns, "
            + "mode, description, freshness (time of the latest record, records not yet materialized) and an example query.",
            annotations = @Tool.Annotations(readOnlyHint = true, openWorldHint = false))
    String getMetadata(@ToolArg(name = "topicName", description = "Table (topic) name, e.g. customer_360") String topicName) {
        try {
            return json(queries.metadata(Access.Caller.of(identity), topicName));
        } catch (QueryException e) {
            throw new ToolCallException(e.getMessage());
        }
    }

    @Tool(name = "queryData", description = QUERY_HELP,
            annotations = @Tool.Annotations(readOnlyHint = true, openWorldHint = false))
    String queryData(@ToolArg(name = "query", description = "SQL query, e.g. SELECT * FROM customer_360 WHERE customer_id = 42") String query) {
        try {
            return json(queries.query(Access.Caller.of(identity), AuditLog.Channel.MCP, query));
        } catch (QueryException e) {
            throw new ToolCallException(e.getMessage());
        }
    }

    private String json(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new ToolCallException(e);
        }
    }
}

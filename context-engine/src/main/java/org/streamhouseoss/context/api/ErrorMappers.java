package org.streamhouseoss.context.api;

import java.util.Map;

import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.streamhouseoss.context.ingest.MaterializerManager;
import org.streamhouseoss.context.query.QueryException;

/** Maps domain errors to HTTP responses with a JSON {@code error} message. */
public class ErrorMappers {

    @ServerExceptionMapper
    public RestResponse<Map<String, String>> query(QueryException e) {
        RestResponse.Status status = switch (e.reason()) {
            case INVALID -> RestResponse.Status.BAD_REQUEST;
            case NOT_FOUND -> RestResponse.Status.NOT_FOUND;
            case FORBIDDEN -> RestResponse.Status.FORBIDDEN;
            case TIMEOUT -> RestResponse.Status.SERVICE_UNAVAILABLE;
        };
        return RestResponse.status(status, Map.of("error", e.getMessage()));
    }

    @ServerExceptionMapper
    public RestResponse<Map<String, String>> enable(MaterializerManager.EnableException e) {
        return RestResponse.status(RestResponse.Status.CONFLICT, Map.of("error", e.getMessage()));
    }
}

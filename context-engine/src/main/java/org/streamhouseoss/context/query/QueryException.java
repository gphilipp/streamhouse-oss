package org.streamhouseoss.context.query;

/** A query the caller can fix: invalid syntax, an unsupported construct, an unknown column. */
public class QueryException extends RuntimeException {

    public enum Reason {
        INVALID, NOT_FOUND, FORBIDDEN, TIMEOUT
    }

    private final Reason reason;

    public QueryException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public static QueryException invalid(String message) {
        return new QueryException(Reason.INVALID, message);
    }

    public Reason reason() {
        return reason;
    }
}

package org.streamhouseoss.cli;

import com.fasterxml.jackson.databind.JsonNode;

/** An error to print to the user (no stack trace) with a non-zero exit code. */
class CliException extends RuntimeException {

    private final transient JsonNode body;

    CliException(String message) {
        this(message, null);
    }

    CliException(String message, JsonNode body) {
        super(message);
        this.body = body;
    }

    JsonNode body() {
        return body;
    }
}

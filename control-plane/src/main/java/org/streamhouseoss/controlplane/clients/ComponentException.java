package org.streamhouseoss.controlplane.clients;

/** A call to a platform component (Connect, Flink, Gravitino, ...) failed. */
public class ComponentException extends RuntimeException {

    public ComponentException(String message) {
        super(message);
    }

    public ComponentException(String message, Throwable cause) {
        super(message, cause);
    }
}

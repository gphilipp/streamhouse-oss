package org.streamhouseoss.controlplane.state;

/** A statement that contradicts the current desired state (duplicate, dangling reference, dependents). */
public class ConflictException extends RuntimeException {

    public ConflictException(String message) {
        super(message);
    }
}

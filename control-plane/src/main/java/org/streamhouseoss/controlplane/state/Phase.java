package org.streamhouseoss.controlplane.state;

public enum Phase {
    /** Accepted; waiting for a dependency (a topic, a schema) or for the first reconcile. */
    PENDING,
    /** Running as declared. */
    READY,
    /** The last reconcile failed; the message says why. Retried on the next loop. */
    FAILED,
    /** Dropped; external resources are being removed. */
    DELETING
}

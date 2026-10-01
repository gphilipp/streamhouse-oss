package org.streamhouseoss.context.store;

public enum TableStatus {
    /** Enabled; the serving table is created when the first record arrives. */
    WAITING_FOR_DATA,
    /** Materializing; queries return current state. */
    ACTIVE,
    /** Stopped on an error that needs attention (see the status message); re-enable to retry. */
    FAILED
}

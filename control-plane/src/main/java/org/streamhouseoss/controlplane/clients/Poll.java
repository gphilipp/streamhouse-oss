package org.streamhouseoss.controlplane.clients;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

/** Waits for a condition, the one blocking wait used by the component clients. */
public final class Poll {

    private Poll() {
    }

    public static void until(BooleanSupplier condition, Duration timeout, Duration interval, String what) {
        Instant deadline = Instant.now().plus(timeout);
        while (!condition.getAsBoolean()) {
            if (Instant.now().isAfter(deadline)) {
                throw new ComponentException("timed out waiting for " + what);
            }
            sleep(interval);
        }
    }

    public static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ComponentException("interrupted", e);
        }
    }
}

package org.streamhouseoss.context.store;

public class StoreException extends RuntimeException {

    public StoreException(String message, Throwable cause) {
        super(message + ": " + cause.getMessage(), cause);
    }
}

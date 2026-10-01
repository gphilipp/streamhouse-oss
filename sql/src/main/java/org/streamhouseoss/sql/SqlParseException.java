package org.streamhouseoss.sql;

public class SqlParseException extends RuntimeException {

    private final int line;

    public SqlParseException(String message, int line) {
        super("line " + line + ": " + message);
        this.line = line;
    }

    public int line() {
        return line;
    }
}

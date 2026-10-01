package org.streamhouseoss.sql;

/** Builds a statement once its full text span is known. */
@FunctionalInterface
interface StatementBuilder {
    Statement build(String text);
}

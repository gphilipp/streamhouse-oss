package org.streamhouseoss.model;

import java.util.regex.Pattern;

final class Names {
    /** Lower-case SQL-ish identifiers; they become Kafka topic, Postgres and Iceberg names. */
    static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]{0,62}");
    /** Topic names may additionally contain dots (e.g. {@code shop.public.orders}). */
    static final Pattern TOPIC = Pattern.compile("[a-z_][a-z0-9_.]{0,199}");
    /** Flink statement names: lower-case words joined by dashes or dots, at most 100 characters. */
    static final Pattern STATEMENT = Pattern.compile("(?=.{1,100}$)[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");

    private Names() {
    }
}

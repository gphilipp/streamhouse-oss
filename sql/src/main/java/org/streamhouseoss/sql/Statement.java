package org.streamhouseoss.sql;

import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

/** A parsed streamhouse SQL statement. {@link #text()} is the original statement text. */
public sealed interface Statement {

    String text();

    /** Creates or replaces a resource (CREATE ..., ALTER TOPIC ... ENABLE ..., GRANT ...). */
    record Apply(Resource resource, boolean orReplace, String text) implements Statement {
    }

    /** Removes a resource (DROP ..., ALTER TOPIC ... DISABLE ..., REVOKE ...). */
    record Remove(ResourceKind kind, String name, boolean ifExists, String text) implements Statement {
    }

    /** Lists resources of a kind. A {@code null} kind means Kafka topics. */
    record Show(ResourceKind kind, String text) implements Statement {
    }

    /** Shows one resource with its status and lineage. */
    record Describe(ResourceKind kind, String name, String text) implements Statement {
    }
}

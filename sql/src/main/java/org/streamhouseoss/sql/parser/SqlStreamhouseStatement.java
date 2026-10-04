package org.streamhouseoss.sql.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A parsed streamhouse DDL statement, as produced by the grammar in {@code parserImpls.ftl}. It
 * only records what was written; {@link org.streamhouseoss.sql.SqlParser} validates it and turns it
 * into the resource model.
 */
public final class SqlStreamhouseStatement extends SqlCall {

    public enum Verb {
        CREATE_CONNECTION, CREATE_SOURCE, CREATE_MATERIALIZED_VIEW, CREATE_STATEMENT,
        DROP, ENABLE, DISABLE, GRANT, REVOKE, SHOW, DESCRIBE
    }

    /** {@code key = value} in a WITH clause; {@code secret} for {@code key = SECRET 'ref'}. */
    public record Option(String key, SqlNode value, boolean secret, SqlParserPos pos) {
    }

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("STREAMHOUSE DDL", SqlKind.OTHER_DDL);

    public final Verb verb;
    /** The object kind for DROP, SHOW, DESCRIBE, ENABLE and DISABLE: CONNECTION, SOURCE, ICEBERG, ... */
    public String object;
    public boolean replace;
    public boolean ifExists;
    public SqlIdentifier name;
    /** The connection type, the source's connection, or the grant's role. */
    public SqlIdentifier target;
    /** Source tables or primary key columns. */
    public final List<SqlIdentifier> identifiers = new ArrayList<>();
    public final List<Option> options = new ArrayList<>();
    /** Position of the embedded Flink SQL after AS, if any. */
    public SqlParserPos raw;
    private SqlParserPos pos = SqlParserPos.ZERO;

    public SqlStreamhouseStatement(Verb verb) {
        super(SqlParserPos.ZERO);
        this.verb = verb;
    }

    public SqlStreamhouseStatement at(SqlParserPos pos) {
        this.pos = pos;
        return this;
    }

    @Override
    public SqlParserPos getParserPosition() {
        return pos;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of();
    }
}

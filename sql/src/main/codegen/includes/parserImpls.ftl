<#--
  Streamhouse DDL productions, included into Calcite's Parser.jj.

  CREATE [OR REPLACE] CONNECTION name TYPE type [WITH (key = value | SECRET 'ref', ...)]
  CREATE [OR REPLACE] SOURCE name FROM CONNECTION name TABLES (schema.table, ...)
  CREATE [OR REPLACE] MATERIALIZED VIEW name PRIMARY KEY (col, ...) AS <Flink SQL query>
  CREATE [OR REPLACE] STATEMENT name AS <Flink SQL statement>
  DROP CONNECTION | SOURCE | MATERIALIZED VIEW | STATEMENT [IF EXISTS] name
  ALTER TOPIC topic ENABLE | DISABLE ICEBERG | CONTEXT [WITH (...)]
  GRANT SELECT ON CONTEXT topic TO ROLE role
  REVOKE SELECT ON CONTEXT topic FROM ROLE role
  SHOW TOPICS | CONNECTIONS | SOURCES | MATERIALIZED VIEWS | STATEMENTS | ICEBERG TABLES | CONTEXT TABLES | GRANTS
  DESCRIBE CONNECTION | SOURCE | MATERIALIZED VIEW | STATEMENT | ICEBERG | CONTEXT name

  Embedded Flink SQL (after AS) is not parsed here: its position is recorded so the caller can
  keep the original text, which Flink parses with its own dialect.
-->

SqlNode SqlStreamhouseCreate() :
{
    final Span s;
    final SqlStreamhouseStatement st;
    boolean replace = false;
    SqlIdentifier id;
}
{
    <CREATE> { s = span(); }
    [ <OR> <REPLACE> { replace = true; } ]
    (
        <CONNECTION> { st = new SqlStreamhouseStatement(Verb.CREATE_CONNECTION); }
        id = SimpleIdentifier() { st.name = id; }
        <TYPE> id = SimpleIdentifier() { st.target = id; }
        [ <WITH> StreamhouseOptions(st) ]
    |
        <SOURCE> { st = new SqlStreamhouseStatement(Verb.CREATE_SOURCE); }
        id = SimpleIdentifier() { st.name = id; }
        <FROM> <CONNECTION> id = SimpleIdentifier() { st.target = id; }
        <TABLES> <LPAREN>
        id = CompoundIdentifier() { st.identifiers.add(id); }
        ( <COMMA> id = CompoundIdentifier() { st.identifiers.add(id); } )*
        <RPAREN>
    |
        <MATERIALIZED> <VIEW> { st = new SqlStreamhouseStatement(Verb.CREATE_MATERIALIZED_VIEW); }
        id = SimpleIdentifier() { st.name = id; }
        <PRIMARY> <KEY> <LPAREN>
        id = SimpleIdentifier() { st.identifiers.add(id); }
        ( <COMMA> id = SimpleIdentifier() { st.identifiers.add(id); } )*
        <RPAREN>
        <AS> StreamhouseRawTail(st)
    |
        <STATEMENT> { st = new SqlStreamhouseStatement(Verb.CREATE_STATEMENT); }
        id = SimpleIdentifier() { st.name = id; }
        <AS> StreamhouseRawTail(st)
    )
    {
        st.replace = replace;
        return st.at(s.end(this));
    }
}

SqlNode SqlStreamhouseDrop() :
{
    final Span s;
    final SqlStreamhouseStatement st = new SqlStreamhouseStatement(Verb.DROP);
    SqlIdentifier id;
}
{
    <DROP> { s = span(); }
    StreamhouseObjectKind(st)
    [ LOOKAHEAD(2) <IF> <EXISTS> { st.ifExists = true; } ]
    id = SimpleIdentifier() { st.name = id; }
    { return st.at(s.end(this)); }
}

SqlNode SqlStreamhouseAlterTopic() :
{
    final Span s;
    final SqlStreamhouseStatement st;
    SqlIdentifier topic;
}
{
    <ALTER> { s = span(); } <TOPIC>
    topic = CompoundIdentifier()
    (
        <ENABLE> { st = new SqlStreamhouseStatement(Verb.ENABLE); }
    |
        <DISABLE> { st = new SqlStreamhouseStatement(Verb.DISABLE); }
    )
    ( <ICEBERG> { st.object = "ICEBERG"; } | <CONTEXT> { st.object = "CONTEXT"; } )
    [ <WITH> StreamhouseOptions(st) ]
    {
        st.name = topic;
        return st.at(s.end(this));
    }
}

SqlNode SqlStreamhouseGrant() :
{
    final Span s;
    final SqlStreamhouseStatement st = new SqlStreamhouseStatement(Verb.GRANT);
    SqlIdentifier id;
}
{
    <GRANT> { s = span(); } <SELECT> <ON> <CONTEXT>
    id = CompoundIdentifier() { st.name = id; }
    <TO> <ROLE> id = SimpleIdentifier() { st.target = id; }
    { return st.at(s.end(this)); }
}

SqlNode SqlStreamhouseRevoke() :
{
    final Span s;
    final SqlStreamhouseStatement st = new SqlStreamhouseStatement(Verb.REVOKE);
    SqlIdentifier id;
}
{
    <REVOKE> { s = span(); } <SELECT> <ON> <CONTEXT>
    id = CompoundIdentifier() { st.name = id; }
    <FROM> <ROLE> id = SimpleIdentifier() { st.target = id; }
    { return st.at(s.end(this)); }
}

SqlNode SqlStreamhouseShow() :
{
    final Span s;
    final SqlStreamhouseStatement st = new SqlStreamhouseStatement(Verb.SHOW);
}
{
    <SHOW> { s = span(); }
    (
        <TOPICS> { st.object = "TOPICS"; }
    |   <CONNECTIONS> { st.object = "CONNECTION"; }
    |   <SOURCES> { st.object = "SOURCE"; }
    |   <MATERIALIZED> <VIEWS> { st.object = "MATERIALIZED VIEW"; }
    |   <STATEMENTS> { st.object = "STATEMENT"; }
    |   <ICEBERG> <TABLES> { st.object = "ICEBERG"; }
    |   <CONTEXT> <TABLES> { st.object = "CONTEXT"; }
    |   <GRANTS> { st.object = "GRANT"; }
    )
    { return st.at(s.end(this)); }
}

SqlNode SqlStreamhouseDescribe() :
{
    final Span s;
    final SqlStreamhouseStatement st = new SqlStreamhouseStatement(Verb.DESCRIBE);
    SqlIdentifier id;
}
{
    <DESCRIBE> { s = span(); }
    (
        StreamhouseObjectKind(st)
    |   <ICEBERG> { st.object = "ICEBERG"; }
    |   <CONTEXT> { st.object = "CONTEXT"; }
    )
    id = CompoundIdentifier() { st.name = id; }
    { return st.at(s.end(this)); }
}

void StreamhouseObjectKind(SqlStreamhouseStatement st) :
{
}
{
        <CONNECTION> { st.object = "CONNECTION"; }
    |   <SOURCE> { st.object = "SOURCE"; }
    |   <MATERIALIZED> <VIEW> { st.object = "MATERIALIZED VIEW"; }
    |   <STATEMENT> { st.object = "STATEMENT"; }
}

void StreamhouseOptions(SqlStreamhouseStatement st) :
{
}
{
    <LPAREN> StreamhouseOption(st) ( <COMMA> StreamhouseOption(st) )* <RPAREN>
}

void StreamhouseOption(SqlStreamhouseStatement st) :
{
    final String key;
    final SqlParserPos pos;
    SqlNode value;
    boolean secret = false;
}
{
    { pos = getPos(); }
    key = StreamhouseOptionKey()
    <EQ>
    (
        <SECRET> value = StringLiteral() { secret = true; }
    |
        value = Literal()
    )
    { st.options.add(new SqlStreamhouseStatement.Option(key, value, secret, pos)); }
}

/**
 * An option name: words joined by dots (reserved words such as USER allowed), or a quoted string,
 * e.g. user, publication, 'key.format'.
 */
JAVACODE String StreamhouseOptionKey() {
    final StringBuilder key = new StringBuilder();
    while (true) {
        final Token t = getNextToken();
        if (t.kind == QUOTED_STRING) {
            key.append(SqlParserUtil.parseString(t.image));
        } else if (t.kind == BACK_QUOTED_IDENTIFIER) {
            key.append(t.image.substring(1, t.image.length() - 1).replace("``", "`"));
        } else if (!t.image.isEmpty() && (Character.isLetter(t.image.charAt(0)) || t.image.charAt(0) == '_')) {
            key.append(t.image.toLowerCase(Locale.ROOT));
        } else {
            throw new ParseException("expected an option name at line " + t.beginLine + ", column "
                + t.beginColumn + " (found '" + t.image + "')");
        }
        if (getToken(1).kind != DOT) {
            return key.toString();
        }
        getNextToken();
        key.append('.');
    }
}

/**
 * Records the position of everything up to the end of the statement (the next top-level ';'
 * or the end of input) without parsing it: embedded Flink SQL is kept as written.
 */
JAVACODE void StreamhouseRawTail(SqlStreamhouseStatement st) {
    final Token first = getToken(1);
    if (first.kind == EOF || first.kind == SEMICOLON) {
        throw new ParseException("expected a Flink SQL statement after AS at line " + first.beginLine
            + ", column " + first.beginColumn);
    }
    Token last = first;
    while (getToken(1).kind != EOF && getToken(1).kind != SEMICOLON) {
        last = getNextToken();
    }
    st.raw = new SqlParserPos(first.beginLine, first.beginColumn, last.endLine, last.endColumn);
}

package org.streamhouseoss.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.streamhouseoss.model.ConnectionType;
import org.streamhouseoss.model.OptionValue;
import org.streamhouseoss.model.Privilege;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.model.TableMode;
import org.streamhouseoss.model.TableRef;
import org.streamhouseoss.sql.Lexer.Token;
import org.streamhouseoss.sql.Lexer.Type;

/**
 * Recursive-descent parser for the streamhouse DDL.
 *
 * <pre>
 * CREATE [OR REPLACE] CONNECTION name TYPE POSTGRES [WITH (key = 'v' | SECRET 'ref', ...)]
 * CREATE [OR REPLACE] SOURCE name FROM CONNECTION name TABLES (schema.table, ...)
 * CREATE [OR REPLACE] MATERIALIZED VIEW name PRIMARY KEY (col, ...) AS &lt;Flink SQL query&gt;
 * ALTER TOPIC topic ENABLE TABLEFLOW [WITH (mode = 'append' | 'upsert')]
 * ALTER TOPIC topic ENABLE CONTEXT [WITH (mode = ..., description = '...')]
 * ALTER TOPIC topic DISABLE TABLEFLOW | CONTEXT
 * GRANT SELECT ON CONTEXT topic TO ROLE role
 * REVOKE SELECT ON CONTEXT topic FROM ROLE role
 * DROP CONNECTION | SOURCE | MATERIALIZED VIEW [IF EXISTS] name
 * SHOW TOPICS | CONNECTIONS | SOURCES | MATERIALIZED VIEWS | TABLEFLOWS | CONTEXT TABLES | GRANTS
 * DESCRIBE CONNECTION | SOURCE | MATERIALIZED VIEW | TABLEFLOW | CONTEXT name
 * </pre>
 *
 * Unquoted identifiers are case-insensitive and normalized to lower case.
 */
public final class SqlParser {

    private final String input;
    private final List<Token> tokens;
    private int pos;

    private SqlParser(String input) {
        this.input = input;
        this.tokens = new Lexer(input).tokenize();
    }

    /** Parses a script of {@code ;}-separated statements. */
    public static List<Statement> parseScript(String script) {
        return new SqlParser(script).script();
    }

    /** Parses exactly one statement (a trailing {@code ;} is allowed). */
    public static Statement parseStatement(String sql) {
        List<Statement> statements = parseScript(sql);
        if (statements.size() != 1) {
            throw new SqlParseException("expected exactly one statement, got " + statements.size(), 1);
        }
        return statements.getFirst();
    }

    private List<Statement> script() {
        List<Statement> statements = new ArrayList<>();
        while (peek().type() != Type.EOF) {
            if (peek().type() == Type.SEMICOLON) {
                pos++;
                continue;
            }
            int start = peek().start();
            StatementBuilder builder = statement();
            int end = tokens.get(pos - 1).end();
            if (peek().type() != Type.EOF) {
                expect(Type.SEMICOLON, "';'");
            }
            statements.add(builder.build(input.substring(start, end)));
        }
        return statements;
    }

    private StatementBuilder statement() {
        Token t = peek();
        if (acceptWord("CREATE")) {
            return create();
        }
        if (acceptWord("ALTER")) {
            return alterTopic();
        }
        if (acceptWord("GRANT")) {
            return grant();
        }
        if (acceptWord("REVOKE")) {
            return revoke();
        }
        if (acceptWord("DROP")) {
            return drop();
        }
        if (acceptWord("SHOW")) {
            return show();
        }
        if (acceptWord("DESCRIBE")) {
            ResourceKind kind = objectKind();
            String name = kind == ResourceKind.TABLEFLOW || kind == ResourceKind.CONTEXT_TABLE ? topicName() : identifier();
            return text -> new Statement.Describe(kind, name, text);
        }
        throw error(t, "expected CREATE, ALTER, GRANT, REVOKE, DROP, SHOW or DESCRIBE");
    }

    private StatementBuilder create() {
        boolean orReplace = false;
        if (acceptWord("OR")) {
            expectWord("REPLACE");
            orReplace = true;
        }
        boolean replace = orReplace;
        if (acceptWord("CONNECTION")) {
            String name = identifier();
            expectWord("TYPE");
            Token typeToken = peek();
            String typeName = identifier();
            ConnectionType type;
            try {
                type = ConnectionType.valueOf(typeName.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw error(typeToken, "unsupported connection type " + typeName);
            }
            Map<String, OptionValue> options = acceptWord("WITH") ? options() : Map.of();
            Resource resource = build(typeToken, () -> new Resource.Connection(name, type, options));
            return text -> new Statement.Apply(resource, replace, text);
        }
        if (acceptWord("SOURCE")) {
            Token nameToken = peek();
            String name = identifier();
            expectWord("FROM");
            expectWord("CONNECTION");
            String connection = identifier();
            expectWord("TABLES");
            expectSymbol("(");
            List<TableRef> tables = new ArrayList<>();
            do {
                Token tableToken = peek();
                String schema = identifier();
                expectSymbol(".");
                String table = identifier();
                tables.add(build(tableToken, () -> new TableRef(schema, table)));
            } while (acceptSymbol(","));
            expectSymbol(")");
            Resource resource = build(nameToken, () -> new Resource.Source(name, connection, tables));
            return text -> new Statement.Apply(resource, replace, text);
        }
        if (acceptWord("MATERIALIZED")) {
            expectWord("VIEW");
            Token nameToken = peek();
            String name = identifier();
            expectWord("PRIMARY");
            expectWord("KEY");
            List<String> primaryKey = identifierList();
            expectWord("AS");
            String query = rawUntilEndOfStatement();
            Resource resource = build(nameToken, () -> new Resource.MaterializedView(name, primaryKey, query));
            return text -> new Statement.Apply(resource, replace, text);
        }
        throw error(peek(), "expected CONNECTION, SOURCE or MATERIALIZED VIEW");
    }

    private StatementBuilder alterTopic() {
        expectWord("TOPIC");
        Token topicToken = peek();
        String topic = topicName();
        if (acceptWord("ENABLE")) {
            if (acceptWord("TABLEFLOW")) {
                Map<String, String> options = literalOptions(Map.of("mode", "upsert"), "mode");
                TableMode mode = tableMode(topicToken, options.get("mode"));
                Resource resource = build(topicToken, () -> new Resource.Tableflow(topic, mode));
                return text -> new Statement.Apply(resource, true, text);
            }
            expectWord("CONTEXT");
            Map<String, String> options = literalOptions(Map.of(), "mode", "description");
            TableMode mode = options.containsKey("mode") ? tableMode(topicToken, options.get("mode")) : null;
            Resource resource = build(topicToken, () -> new Resource.ContextTable(topic, mode, options.get("description")));
            return text -> new Statement.Apply(resource, true, text);
        }
        expectWord("DISABLE");
        ResourceKind kind;
        if (acceptWord("TABLEFLOW")) {
            kind = ResourceKind.TABLEFLOW;
        } else {
            expectWord("CONTEXT");
            kind = ResourceKind.CONTEXT_TABLE;
        }
        return text -> new Statement.Remove(kind, topic, true, text);
    }

    private StatementBuilder grant() {
        Token start = peek();
        Privilege privilege = privilege();
        expectWord("ON");
        expectWord("CONTEXT");
        String topic = topicName();
        expectWord("TO");
        expectWord("ROLE");
        String role = identifier();
        Resource resource = build(start, () -> new Resource.Grant(privilege, ResourceKind.CONTEXT_TABLE, topic, role));
        return text -> new Statement.Apply(resource, true, text);
    }

    private StatementBuilder revoke() {
        Token start = peek();
        Privilege privilege = privilege();
        expectWord("ON");
        expectWord("CONTEXT");
        String topic = topicName();
        expectWord("FROM");
        expectWord("ROLE");
        String role = identifier();
        Resource.Grant grant = build(start, () -> new Resource.Grant(privilege, ResourceKind.CONTEXT_TABLE, topic, role));
        return text -> new Statement.Remove(ResourceKind.GRANT, grant.name(), true, text);
    }

    private StatementBuilder drop() {
        ResourceKind kind;
        if (acceptWord("CONNECTION")) {
            kind = ResourceKind.CONNECTION;
        } else if (acceptWord("SOURCE")) {
            kind = ResourceKind.SOURCE;
        } else if (acceptWord("MATERIALIZED")) {
            expectWord("VIEW");
            kind = ResourceKind.MATERIALIZED_VIEW;
        } else {
            throw error(peek(), "expected CONNECTION, SOURCE or MATERIALIZED VIEW");
        }
        boolean ifExists = false;
        if (acceptWord("IF")) {
            expectWord("EXISTS");
            ifExists = true;
        }
        boolean exists = ifExists;
        String name = identifier();
        return text -> new Statement.Remove(kind, name, exists, text);
    }

    private StatementBuilder show() {
        ResourceKind kind;
        if (acceptWord("TOPICS")) {
            kind = null;
        } else if (acceptWord("CONNECTIONS")) {
            kind = ResourceKind.CONNECTION;
        } else if (acceptWord("SOURCES")) {
            kind = ResourceKind.SOURCE;
        } else if (acceptWord("MATERIALIZED")) {
            expectWord("VIEWS");
            kind = ResourceKind.MATERIALIZED_VIEW;
        } else if (acceptWord("TABLEFLOWS")) {
            kind = ResourceKind.TABLEFLOW;
        } else if (acceptWord("CONTEXT")) {
            expectWord("TABLES");
            kind = ResourceKind.CONTEXT_TABLE;
        } else if (acceptWord("GRANTS")) {
            kind = ResourceKind.GRANT;
        } else {
            throw error(peek(), "expected TOPICS, CONNECTIONS, SOURCES, MATERIALIZED VIEWS, TABLEFLOWS, CONTEXT TABLES or GRANTS");
        }
        return text -> new Statement.Show(kind, text);
    }

    private ResourceKind objectKind() {
        if (acceptWord("CONNECTION")) {
            return ResourceKind.CONNECTION;
        }
        if (acceptWord("SOURCE")) {
            return ResourceKind.SOURCE;
        }
        if (acceptWord("MATERIALIZED")) {
            expectWord("VIEW");
            return ResourceKind.MATERIALIZED_VIEW;
        }
        if (acceptWord("TABLEFLOW")) {
            return ResourceKind.TABLEFLOW;
        }
        if (acceptWord("CONTEXT")) {
            return ResourceKind.CONTEXT_TABLE;
        }
        throw error(peek(), "expected CONNECTION, SOURCE, MATERIALIZED VIEW, TABLEFLOW or CONTEXT");
    }

    private Privilege privilege() {
        Token t = peek();
        String word = identifier();
        try {
            return Privilege.valueOf(word.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw error(t, "unsupported privilege " + word);
        }
    }

    private TableMode tableMode(Token at, String value) {
        try {
            return TableMode.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw error(at, "mode must be 'append' or 'upsert', got '" + value + "'");
        }
    }

    /** {@code WITH (key = value, ...)}; values may be secret references. */
    private Map<String, OptionValue> options() {
        Map<String, OptionValue> options = new LinkedHashMap<>();
        expectSymbol("(");
        do {
            Token keyToken = peek();
            String key = optionKey();
            expectSymbol("=");
            OptionValue value;
            if (acceptWord("SECRET")) {
                value = new OptionValue.Secret(expect(Type.STRING, "secret reference").text());
            } else {
                value = new OptionValue.Literal(literal());
            }
            if (options.put(key, value) != null) {
                throw error(keyToken, "duplicate option " + key);
            }
        } while (acceptSymbol(","));
        expectSymbol(")");
        return options;
    }

    /** Optional {@code WITH (...)} clause restricted to literal values and the given keys. */
    private Map<String, String> literalOptions(Map<String, String> defaults, String... allowed) {
        Map<String, String> result = new LinkedHashMap<>(defaults);
        if (!acceptWord("WITH")) {
            return result;
        }
        Token start = peek();
        for (Map.Entry<String, OptionValue> e : options().entrySet()) {
            if (!List.of(allowed).contains(e.getKey())) {
                throw error(start, "unknown option " + e.getKey() + "; allowed: " + String.join(", ", allowed));
            }
            if (!(e.getValue() instanceof OptionValue.Literal literal)) {
                throw error(start, "option " + e.getKey() + " cannot be a secret");
            }
            result.put(e.getKey(), literal.value());
        }
        return result;
    }

    private String optionKey() {
        Token t = peek();
        if (t.type() == Type.STRING) {
            pos++;
            return t.text();
        }
        StringBuilder key = new StringBuilder(identifier());
        while (acceptSymbol(".")) {
            key.append('.').append(identifier());
        }
        return key.toString();
    }

    private String literal() {
        Token t = peek();
        if (t.type() == Type.STRING || t.type() == Type.NUMBER) {
            pos++;
            return t.text();
        }
        if (t.isWord("TRUE") || t.isWord("FALSE")) {
            pos++;
            return t.text().toLowerCase(Locale.ROOT);
        }
        throw error(t, "expected a string, number or boolean");
    }

    private List<String> identifierList() {
        List<String> names = new ArrayList<>();
        expectSymbol("(");
        do {
            names.add(identifier());
        } while (acceptSymbol(","));
        expectSymbol(")");
        return names;
    }

    /** A possibly dotted topic name such as {@code shop.public.orders}, or a quoted identifier. */
    private String topicName() {
        StringBuilder name = new StringBuilder(identifier());
        while (acceptSymbol(".")) {
            name.append('.').append(identifier());
        }
        return name.toString();
    }

    private String identifier() {
        Token t = peek();
        if (t.type() == Type.WORD) {
            pos++;
            return t.text().toLowerCase(Locale.ROOT);
        }
        if (t.type() == Type.QUOTED_IDENTIFIER) {
            pos++;
            return t.text();
        }
        throw error(t, "expected an identifier");
    }

    /** Everything from the current token up to (excluding) the statement-terminating {@code ;}. */
    private String rawUntilEndOfStatement() {
        int start = peek().start();
        int end = start;
        while (peek().type() != Type.SEMICOLON && peek().type() != Type.EOF) {
            end = tokens.get(pos++).end();
        }
        if (end == start) {
            throw error(peek(), "expected a query");
        }
        return input.substring(start, end);
    }

    private <T> T build(Token at, java.util.function.Supplier<T> constructor) {
        try {
            return constructor.get();
        } catch (IllegalArgumentException e) {
            throw error(at, e.getMessage());
        }
    }

    private Token peek() {
        return tokens.get(pos);
    }

    private boolean acceptWord(String word) {
        if (peek().isWord(word)) {
            pos++;
            return true;
        }
        return false;
    }

    private boolean acceptSymbol(String symbol) {
        if (peek().isSymbol(symbol)) {
            pos++;
            return true;
        }
        return false;
    }

    private void expectWord(String word) {
        if (!acceptWord(word)) {
            throw error(peek(), "expected " + word);
        }
    }

    private void expectSymbol(String symbol) {
        if (!acceptSymbol(symbol)) {
            throw error(peek(), "expected '" + symbol + "'");
        }
    }

    private Token expect(Type type, String what) {
        Token t = peek();
        if (t.type() != type) {
            throw error(t, "expected " + what);
        }
        pos++;
        return t;
    }

    private static SqlParseException error(Token at, String message) {
        String found = at.type() == Type.EOF ? "end of input" : "'" + at.text() + "'";
        return new SqlParseException(message + " (found " + found + ")", at.line());
    }
}

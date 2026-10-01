package org.streamhouseoss.sql;

import java.util.ArrayList;
import java.util.List;

/** Tokenizes a streamhouse SQL script. Positions are kept so raw query text can be sliced out. */
final class Lexer {

    enum Type {
        WORD, QUOTED_IDENTIFIER, STRING, NUMBER, SYMBOL, SEMICOLON, EOF
    }

    record Token(Type type, String text, int start, int end, int line) {
        boolean isWord(String word) {
            return type == Type.WORD && text.equalsIgnoreCase(word);
        }

        boolean isSymbol(String symbol) {
            return type == Type.SYMBOL && text.equals(symbol);
        }
    }

    private final String input;
    private int pos;
    private int line = 1;

    Lexer(String input) {
        this.input = input;
    }

    List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        while (true) {
            skipWhitespaceAndComments();
            if (pos >= input.length()) {
                tokens.add(new Token(Type.EOF, "", pos, pos, line));
                return tokens;
            }
            tokens.add(next());
        }
    }

    private Token next() {
        int start = pos;
        char c = input.charAt(pos);
        if (Character.isLetter(c) || c == '_') {
            while (pos < input.length() && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_')) {
                pos++;
            }
            return new Token(Type.WORD, input.substring(start, pos), start, pos, line);
        }
        if (Character.isDigit(c)) {
            while (pos < input.length() && (Character.isDigit(input.charAt(pos)) || input.charAt(pos) == '.')) {
                pos++;
            }
            return new Token(Type.NUMBER, input.substring(start, pos), start, pos, line);
        }
        if (c == '\'') {
            return new Token(Type.STRING, quoted('\''), start, pos, line);
        }
        if (c == '"' || c == '`') {
            return new Token(Type.QUOTED_IDENTIFIER, quoted(c), start, pos, line);
        }
        pos++;
        if (c == ';') {
            return new Token(Type.SEMICOLON, ";", start, pos, line);
        }
        return new Token(Type.SYMBOL, String.valueOf(c), start, pos, line);
    }

    /** Reads a quoted literal; the quote character is escaped by doubling it. */
    private String quoted(char quote) {
        int startLine = line;
        StringBuilder sb = new StringBuilder();
        pos++;
        while (pos < input.length()) {
            char c = input.charAt(pos++);
            if (c == quote) {
                if (pos < input.length() && input.charAt(pos) == quote) {
                    sb.append(quote);
                    pos++;
                    continue;
                }
                return sb.toString();
            }
            if (c == '\n') {
                line++;
            }
            sb.append(c);
        }
        throw new SqlParseException("unterminated quoted text", startLine);
    }

    private void skipWhitespaceAndComments() {
        while (pos < input.length()) {
            char c = input.charAt(pos);
            if (c == '\n') {
                line++;
                pos++;
            } else if (Character.isWhitespace(c)) {
                pos++;
            } else if (input.startsWith("--", pos)) {
                while (pos < input.length() && input.charAt(pos) != '\n') {
                    pos++;
                }
            } else if (input.startsWith("/*", pos)) {
                int startLine = line;
                int end = input.indexOf("*/", pos + 2);
                if (end < 0) {
                    throw new SqlParseException("unterminated comment", startLine);
                }
                line += (int) input.substring(pos, end).chars().filter(ch -> ch == '\n').count();
                pos = end + 2;
            } else {
                return;
            }
        }
    }
}

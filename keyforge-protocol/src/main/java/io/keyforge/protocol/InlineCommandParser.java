package io.keyforge.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Splits an inline command (what a human types into telnet) into arguments, with the same rules as Redis'
 * {@code sdssplitargs}: arguments are separated by whitespace; {@code "double quotes"} support the escapes
 * {@code \n \r \t \b \a \\ \"} and {@code \xHH}; {@code 'single quotes'} only support {@code \'}; a closing quote must be
 * followed by whitespace or the end of the line.
 */
final class InlineCommandParser {

    private static final byte[][] NO_ARGUMENTS = new byte[0][];

    private InlineCommandParser() {
    }

    /** Parses {@code line[from, to)}. Returns an empty array for a blank line. */
    static byte[][] parse(ByteBuffer line, int from, int to) {
        List<byte[]> arguments = new ArrayList<>();
        Argument current = new Argument();
        int p = from;
        while (true) {
            while (p < to && isSpace(line.get(p))) {
                p++;
            }
            if (p == to) {
                return arguments.isEmpty() ? NO_ARGUMENTS : arguments.toArray(byte[][]::new);
            }
            boolean inDoubleQuotes = false;
            boolean inSingleQuotes = false;
            boolean done = false;
            current.reset();
            while (!done) {
                if (p == to) {
                    if (inDoubleQuotes || inSingleQuotes) {
                        throw new RespProtocolException("unbalanced quotes in request");
                    }
                    break;
                }
                byte c = line.get(p);
                if (inDoubleQuotes) {
                    if (c == '\\' && p + 3 < to && line.get(p + 1) == 'x'
                            && isHexDigit(line.get(p + 2)) && isHexDigit(line.get(p + 3))) {
                        current.append((hexValue(line.get(p + 2)) << 4) | hexValue(line.get(p + 3)));
                        p += 3;
                    } else if (c == '\\' && p + 1 < to) {
                        p++;
                        current.append(switch (line.get(p)) {
                            case 'n' -> '\n';
                            case 'r' -> '\r';
                            case 't' -> '\t';
                            case 'b' -> '\b';
                            case 'a' -> 7;
                            default -> line.get(p);
                        });
                    } else if (c == '"') {
                        requireSpaceAfterClosingQuote(line, p, to);
                        done = true;
                    } else {
                        current.append(c);
                    }
                } else if (inSingleQuotes) {
                    if (c == '\\' && p + 1 < to && line.get(p + 1) == '\'') {
                        p++;
                        current.append('\'');
                    } else if (c == '\'') {
                        requireSpaceAfterClosingQuote(line, p, to);
                        done = true;
                    } else {
                        current.append(c);
                    }
                } else {
                    switch (c) {
                        case ' ', '\n', '\r', '\t', 0 -> done = true;
                        case '"' -> inDoubleQuotes = true;
                        case '\'' -> inSingleQuotes = true;
                        default -> current.append(c);
                    }
                }
                p++;
            }
            arguments.add(current.toByteArray());
        }
    }

    private static void requireSpaceAfterClosingQuote(ByteBuffer line, int quote, int to) {
        if (quote + 1 < to && !isSpace(line.get(quote + 1))) {
            throw new RespProtocolException("unbalanced quotes in request");
        }
    }

    private static boolean isSpace(byte c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == 0x0B || c == '\f';
    }

    private static boolean isHexDigit(byte c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private static int hexValue(byte c) {
        return Character.digit(c, 16);
    }

    /** A reusable growable byte buffer for the argument being parsed. */
    private static final class Argument {
        private byte[] bytes = new byte[32];
        private int length;

        void reset() {
            length = 0;
        }

        void append(int b) {
            if (length == bytes.length) {
                bytes = Arrays.copyOf(bytes, length * 2);
            }
            bytes[length++] = (byte) b;
        }

        byte[] toByteArray() {
            return Arrays.copyOf(bytes, length);
        }
    }
}

package io.keyforge.server.command;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import io.keyforge.protocol.RespArray;
import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespValue;

/**
 * CONFIG and COMMAND. These exist so that standard tools start cleanly: {@code redis-benchmark} reads
 * {@code CONFIG GET save} and {@code CONFIG GET appendonly}, and {@code redis-cli} asks for {@code COMMAND DOCS} to
 * build its help.
 */
final class ServerCommands {

    private final Map<String, String> configParameters;
    private final Supplier<CommandTable> commands;

    /**
     * @param configParameters what {@code CONFIG GET} reports, keyed by lowercase Redis parameter name
     * @param commands         the table this command is part of, for {@code COMMAND COUNT} and {@code COMMAND LIST}
     */
    ServerCommands(Map<String, String> configParameters, Supplier<CommandTable> commands) {
        this.configParameters = Map.copyOf(configParameters);
        this.commands = commands;
    }

    List<CommandSpec> specs() {
        return List.of(
                new CommandSpec("config", -2, (session, args) -> config(args)),
                new CommandSpec("command", -1, (session, args) -> command(args)));
    }

    /** Only {@code CONFIG GET parameter [parameter ...]}, with glob patterns ({@code *} and {@code ?}). */
    private RespValue config(byte[][] args) {
        if (!Arguments.is(args[1], "GET")) {
            throw Arguments.unknownSubcommand(args[1], "CONFIG");
        }
        if (args.length < 3) {
            throw Arguments.wrongArity("config|get");
        }
        Map<String, String> matches = new LinkedHashMap<>();
        for (int i = 2; i < args.length; i++) {
            String pattern = Arguments.utf8(args[i]).toLowerCase(Locale.ROOT);
            configParameters.entrySet().stream()
                    .filter(parameter -> globMatches(pattern, parameter.getKey()))
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(parameter -> matches.put(parameter.getKey(), parameter.getValue()));
        }
        List<RespValue> reply = new ArrayList<>(matches.size() * 2);
        matches.forEach((name, value) -> {
            reply.add(RespBulkString.of(name));
            reply.add(RespBulkString.of(value));
        });
        return new RespArray(reply);
    }

    /** {@code COMMAND}, {@code COMMAND DOCS} and {@code COMMAND INFO} return an empty array. */
    private RespValue command(byte[][] args) {
        if (args.length == 1) {
            return RespArray.EMPTY;
        }
        byte[] subcommand = args[1];
        if (Arguments.is(subcommand, "DOCS") || Arguments.is(subcommand, "INFO")) {
            return RespArray.EMPTY;
        }
        if (Arguments.is(subcommand, "COUNT")) {
            if (args.length != 2) {
                throw Arguments.wrongArity("command|count");
            }
            return RespInteger.of(commands.get().size());
        }
        if (Arguments.is(subcommand, "LIST")) {
            List<RespValue> names = new ArrayList<>();
            for (CommandSpec spec : commands.get().commands()) {
                names.add(RespBulkString.of(spec.name()));
            }
            return new RespArray(names);
        }
        throw Arguments.unknownSubcommand(subcommand, "COMMAND");
    }

    /** Glob matching with {@code *} (any run of characters) and {@code ?} (any single character). */
    static boolean globMatches(String pattern, String text) {
        int p = 0;
        int t = 0;
        int starPattern = -1;
        int starText = -1;
        while (t < text.length()) {
            if (p < pattern.length() && (pattern.charAt(p) == '?' || pattern.charAt(p) == text.charAt(t))) {
                p++;
                t++;
            } else if (p < pattern.length() && pattern.charAt(p) == '*') {
                starPattern = p++;
                starText = t;
            } else if (starPattern >= 0) {
                p = starPattern + 1;
                t = ++starText;
            } else {
                return false;
            }
        }
        while (p < pattern.length() && pattern.charAt(p) == '*') {
            p++;
        }
        return p == pattern.length();
    }
}

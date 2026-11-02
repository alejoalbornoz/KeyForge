package io.keyforge.chaos;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Runs external commands, such as the Docker CLI. Tests substitute a recorder. */
interface Shell {

    /**
     * @param exitCode the command's exit code; -1 if it could not be run or did not finish in time
     * @param output   its standard output and standard error, combined
     */
    record Result(int exitCode, String output) {

        boolean succeeded() {
            return exitCode == 0;
        }
    }

    /** Runs {@code command} and waits for it, at most {@code timeout}. Never throws for a failing command. */
    Result run(List<String> command, Duration timeout);

    /** The real thing: a child process per command. */
    static Shell processes() {
        return (command, timeout) -> {
            Process process;
            try {
                process = new ProcessBuilder(command).redirectErrorStream(true).start();
            } catch (IOException e) {
                return new Result(-1, "could not run " + command.getFirst() + ": " + e.getMessage());
            }
            CompletableFuture<String> output = CompletableFuture.supplyAsync(() -> read(process.getInputStream()),
                    Thread::startVirtualThread);
            try {
                if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly();
                    return new Result(-1, "timed out after " + timeout.toMillis() + " ms: " + String.join(" ",
                            command));
                }
                return new Result(process.exitValue(), output.join().strip());
            } catch (InterruptedException e) {
                process.destroyForcibly();
                Thread.currentThread().interrupt();
                return new Result(-1, "interrupted");
            }
        };
    }

    private static String read(InputStream in) {
        try (in) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

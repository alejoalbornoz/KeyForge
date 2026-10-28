package io.keyforge.bench.jmh;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Smoke test for the JMH benchmarks: every benchmark runs once, briefly, in this JVM, with small data sets. It catches a
 * benchmark that no longer compiles against the engine, fails in setup or returns no result; it measures nothing.
 */
@Timeout(300)
class JmhBenchmarksIT {

    @Test
    void putGetAndScanBenchmarksRun() throws RunnerException {
        Options options = new OptionsBuilder()
                .include(LsmPutBenchmark.class.getSimpleName())
                .include(LsmGetBenchmark.class.getSimpleName())
                .include(LsmScanBenchmark.class.getSimpleName())
                .forks(0)
                .threads(1)
                .warmupIterations(0)
                .measurementIterations(1)
                .measurementTime(TimeValue.milliseconds(100))
                .param("fsyncPolicy", "group-commit")
                .param("keyCount", "2000")
                .shouldFailOnError(true)
                .build();

        Collection<RunResult> results = new Runner(options).run();

        // put + put16Threads, getHit + getMiss for two locations, scan for two layouts.
        assertThat(results).hasSize(2 + 4 + 2);
        assertThat(results).allSatisfy(result ->
                assertThat(result.getPrimaryResult().getScore()).as(result.getParams().getBenchmark()).isPositive());
    }
}

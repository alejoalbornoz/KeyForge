package io.keyforge.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.Test;

class BenchOptionsTest {

    @Test
    void defaultsDescribeAClosedLoopRunAgainstALocalServer() {
        BenchOptions options = BenchOptions.parse();

        assertThat(options.host()).isEqualTo("127.0.0.1");
        assertThat(options.port()).isEqualTo(6379);
        assertThat(options.openLoop()).isFalse();
        assertThat(options.intervalNanos()).isZero();
        assertThat(options.warmup()).isEqualTo(Duration.ofSeconds(10));
        assertThat(options.duration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(options.getRatio()).isEqualTo(0.8);
        assertThat(options.prepopulate()).isTrue();
        assertThat(options.output()).isNull();
        assertThat(options.serverPid()).isZero();
    }

    @Test
    void parsesEveryOption() {
        BenchOptions options = BenchOptions.parse("--host=server", "--port=7000", "--connections=50000",
                "--rate=100000", "--warmup-seconds=5", "--duration-seconds=60", "--settle-seconds=2", "--threads=3",
                "--transport=EPOLL", "--connect-concurrency=64", "--keyspace=10", "--value-size=8", "--get-ratio=0.5",
                "--prepopulate=false", "--label=netty", "--output=results/run.csv", "--server-pid=1");

        assertThat(options).isEqualTo(new BenchOptions("server", 7000, 50_000, 100_000, Duration.ofSeconds(5),
                Duration.ofSeconds(60), Duration.ofSeconds(2), 3, "epoll", 64, 10, 8, 0.5, false, "netty",
                Path.of("results/run.csv"), 1));
    }

    @Test
    void openLoopSpreadsTheRateOverConnections() {
        BenchOptions options = BenchOptions.parse("--connections=1000", "--rate=50000");

        assertThat(options.openLoop()).isTrue();
        assertThat(options.intervalNanos()).as("each connection sends every 20 ms").isEqualTo(20_000_000L);
    }

    @Test
    void rejectsUnknownOptionsAndBadValues() {
        assertThatThrownBy(() -> BenchOptions.parse("--conections=5"))
                .hasMessageStartingWith("Unknown option '--conections'");
        assertThatThrownBy(() -> BenchOptions.parse("connections=5"))
                .hasMessageStartingWith("Unexpected argument");
        assertThatThrownBy(() -> BenchOptions.parse("--connections=many"))
                .hasMessage("Option '--connections' must be an integer, got 'many'");
        assertThatThrownBy(() -> BenchOptions.parse("--connections=0"))
                .hasMessage("Option '--connections' must be positive");
        assertThatThrownBy(() -> BenchOptions.parse("--get-ratio=1.5"))
                .hasMessage("Option '--get-ratio' must be between 0 and 1");
        assertThatThrownBy(() -> BenchOptions.parse("--transport=kqueue"))
                .hasMessage("Option '--transport' must be auto, nio or epoll");
        assertThatThrownBy(() -> BenchOptions.parse("--prepopulate=yes"))
                .hasMessage("Option '--prepopulate' must be true or false, got 'yes'");
    }
}

package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

class ChaosOptionsTest {

    @Test
    void defaultsDescribeFiveMinutesOfPartitionsAgainstTheComposeClusterFromItsNetwork() {
        ChaosOptions options = ChaosOptions.parse();

        assertThat(options.nodes()).containsOnlyKeys("kf1", "kf2", "kf3", "kf4", "kf5");
        assertThat(options.nodes().get("kf3").getHostString()).isEqualTo("kf3");
        assertThat(options.nodes().get("kf3").getPort()).isEqualTo(6379);
        assertThat(options.project()).isEqualTo("keyforge");
        assertThat(options.duration()).isEqualTo(Duration.ofMinutes(5));
        assertThat(options.nemesis()).containsExactly(FaultKind.PARTITION);
        assertThat(options.opTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(options.getRatio() + options.incrRatio()).isLessThan(1);
    }

    @Test
    void parsesEveryOption() {
        ChaosOptions options = ChaosOptions.parse("--nodes=a=h1:1,b=h2:2", "--project=p", "--duration-seconds=7",
                "--clients=3", "--rate=40", "--keys=2", "--ops-per-key=9", "--get-ratio=0.2", "--incr-ratio=0.3",
                "--op-timeout-ms=500", "--connect-timeout-ms=50", "--nemesis=kill,pause", "--fault-seconds=4",
                "--calm-seconds=2", "--recovery-seconds=0", "--seed=42", "--output=out/run");

        assertThat(options.nodes()).containsOnlyKeys("a", "b");
        assertThat(options.nodes().get("b").getHostString()).isEqualTo("h2");
        assertThat(options.describe()).containsEntry("nodes", "a=h1:1,b=h2:2").containsEntry("nemesis", "kill,pause")
                .containsEntry("seed", "42").containsEntry("ops-per-key", "9");
        assertThat(options).extracting(ChaosOptions::project, ChaosOptions::duration, ChaosOptions::clients,
                        ChaosOptions::rate, ChaosOptions::keys, ChaosOptions::opsPerKey, ChaosOptions::getRatio,
                        ChaosOptions::incrRatio, ChaosOptions::opTimeout, ChaosOptions::connectTimeout,
                        ChaosOptions::nemesis, ChaosOptions::faultDuration, ChaosOptions::calmDuration,
                        ChaosOptions::recovery, ChaosOptions::seed, ChaosOptions::output)
                .containsExactly("p", Duration.ofSeconds(7), 3, 40, 2, 9, 0.2, 0.3, Duration.ofMillis(500),
                        Duration.ofMillis(50), EnumSet.of(FaultKind.KILL, FaultKind.PAUSE), Duration.ofSeconds(4),
                        Duration.ofSeconds(2), Duration.ZERO, 42L, Path.of("out/run"));
    }

    @Test
    void anEmptySeedIsDrawnAtRandomAndRecorded() {
        ChaosOptions options = ChaosOptions.parse();

        assertThat(options.describe().get("seed")).isEqualTo(Long.toString(options.seed()));
    }

    @Test
    void nemesisShorthands() {
        assertThat(FaultKind.parseList("all")).isEqualTo(FaultKind.ALL).doesNotContain(FaultKind.WIPE);
        assertThat(FaultKind.parseList("none")).isEmpty();
        assertThat(FaultKind.parseList(" Latency , loss ")).containsExactly(FaultKind.LATENCY, FaultKind.LOSS);
        assertThat(FaultKind.describe(Set.of())).isEqualTo("none");
        assertThat(FaultKind.describe(FaultKind.parseList("loss,partition"))).isEqualTo("partition,loss");
    }

    @Test
    void rejectsUnknownOptionsAndBadValues() {
        assertThatThrownBy(() -> ChaosOptions.parse("--client=5"))
                .hasMessageStartingWith("Unknown option '--client'");
        assertThatThrownBy(() -> ChaosOptions.parse("clients=5")).hasMessageStartingWith("Unexpected argument");
        assertThatThrownBy(() -> ChaosOptions.parse("--clients=many"))
                .hasMessage("Option '--clients' must be an integer, got 'many'");
        assertThatThrownBy(() -> ChaosOptions.parse("--rate=0")).hasMessage("Option '--rate' must be positive");
        assertThatThrownBy(() -> ChaosOptions.parse("--get-ratio=0.8", "--incr-ratio=0.3"))
                .hasMessage("Option '--incr-ratio' plus --get-ratio must not exceed 1");
        assertThatThrownBy(() -> ChaosOptions.parse("--nemesis=flood"))
                .hasMessageStartingWith("Option '--nemesis' does not know the fault 'flood'");
        assertThatThrownBy(() -> ChaosOptions.parse("--nodes=kf1"))
                .hasMessage("Option '--nodes' needs name=host:port entries, got 'kf1'");
        assertThatThrownBy(() -> ChaosOptions.parse("--nodes=a=h:1,a=h:2"))
                .hasMessage("Option '--nodes' names a twice");
        assertThatThrownBy(() -> ChaosOptions.parse("--nodes="))
                .hasMessage("Option '--nodes' must name at least one node");
    }
}

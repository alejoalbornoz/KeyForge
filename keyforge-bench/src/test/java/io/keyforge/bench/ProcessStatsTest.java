package io.keyforge.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ProcessStatsTest {

    private static final String STATUS = """
            Name:\tjava
            State:\tS (sleeping)
            Pid:\t1
            VmPeak:\t 4321000 kB
            VmHWM:\t  250000 kB
            VmRSS:\t  200000 kB
            Threads:\t31
            voluntary_ctxt_switches:\t5
            """;

    /** The command name is in parentheses and may itself contain spaces and parentheses. */
    private static final String STAT =
            "1 (java (main) x) S 0 1 1 0 -1 4194560 12345 0 2 0 1500 250 0 0 20 0 31 0 100 4000000 50000 0\n";

    @Test
    void parsesMemoryThreadsAndCpuTime() {
        ProcessStats.Sample sample = ProcessStats.parse(STATUS, STAT);

        assertThat(sample.rssBytes()).isEqualTo(200_000L * 1024);
        assertThat(sample.peakRssBytes()).isEqualTo(250_000L * 1024);
        assertThat(sample.threads()).isEqualTo(31);
        assertThat(sample.cpuTicks()).as("utime + stime").isEqualTo(1750);
    }

    @Test
    void computesAverageCoresBetweenSamples() {
        ProcessStats.Sample before = new ProcessStats.Sample(0, 0, 1, 1_000);
        ProcessStats.Sample after = new ProcessStats.Sample(0, 0, 1, 1_600);

        // 600 ticks = 6 CPU seconds over 3 seconds of wall time.
        assertThat(after.coresSince(before, 3_000_000_000L)).isEqualTo(2.0);
    }

    @Test
    void rejectsAStatusWithoutTheNeededFields() {
        assertThatThrownBy(() -> ProcessStats.parse("Name:\tjava\n", STAT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

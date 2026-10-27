package io.keyforge.bench;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RunResultTest {

    private final RunResult result = new RunResult("netty", "epoll", 1000, 999, 1.25, true, 50_000, 49_987.6,
            1_499_628, 0, 1, 30.0, 85.4, 120.0, 310.2, 990.0, 2_500.0, 4_100.0, 95.5, 60.0, 400.0,
            80.0, 120.5, 300.0, 20.0, 35.5, 12, 14, 1.85, 1.2);

    @Test
    void csvRowHasOneValuePerHeaderColumn() {
        String[] header = RunResult.CSV_HEADER.split(",");
        String[] row = result.toCsv().split(",");

        assertThat(row).hasSameSizeAs(header);
        assertThat(row[0]).isEqualTo("netty");
        assertThat(row[5]).isEqualTo("open");
        assertThat(row[7]).isEqualTo("49987.6");
        assertThat(row[row.length - 1]).isEqualTo("1.20");
    }

    @Test
    void summaryNamesThePercentilesAndServerFigures() {
        String summary = result.summary();

        assertThat(summary).contains("999/1000 connections", "open loop at 50000 req/s", "p99.9 990",
                "RSS 80 MiB before connect", "live heap 20.0 MiB before connect, 35.5 MiB idle",
                "threads 12 idle, 14 peak");
    }
}

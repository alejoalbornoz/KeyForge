package io.keyforge.bench.report;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A latency distribution read from an HdrHistogram percentile file ({@code Histogram.outputPercentileDistribution}):
 * one point per line, {@code Value Percentile TotalCount 1/(1-Percentile)}. The load generator writes values in
 * microseconds.
 */
record PercentileDistribution(List<Point> points) {

    /** A latency (in the file's unit) at a percentile (a fraction from 0 to 1). */
    record Point(double value, double percentile) {
    }

    static PercentileDistribution read(Path file) throws IOException {
        return parse(Files.readAllLines(file, StandardCharsets.UTF_8));
    }

    static PercentileDistribution parse(List<String> lines) {
        List<Point> points = new ArrayList<>();
        for (String line : lines) {
            String[] fields = line.strip().split("\\s+");
            if (fields.length != 4 || line.strip().startsWith("#") || !Character.isDigit(fields[0].charAt(0))) {
                continue; // the header, blank lines and the #[Mean ...] footer
            }
            points.add(new Point(Double.parseDouble(fields[0]), Double.parseDouble(fields[1])));
        }
        if (points.isEmpty()) {
            throw new IllegalArgumentException("No percentile lines found");
        }
        return new PercentileDistribution(List.copyOf(points));
    }

    /** The same distribution with every value multiplied by {@code factor}, for example 0.001 for µs to ms. */
    PercentileDistribution scaled(double factor) {
        return new PercentileDistribution(points.stream()
                .map(point -> new Point(point.value() * factor, point.percentile())).toList());
    }

    /** The highest value recorded (the 100th percentile). */
    double max() {
        return points.getLast().value();
    }
}

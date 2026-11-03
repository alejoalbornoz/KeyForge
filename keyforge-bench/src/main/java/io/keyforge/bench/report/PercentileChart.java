package io.keyforge.bench.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * HdrHistogram's percentile plot: latency (log scale, milliseconds) against percentile, with the percentile axis
 * stretched as {@code log10(1 / (1 - p))} so that 90%, 99%, 99.9% ... are evenly spaced and the tail gets as much room
 * as the median. One line per distribution, labeled at its right end and in the legend.
 *
 * @param title    the headline
 * @param subtitle one line under the title, for the setup
 * @param lines    the distributions; {@code slot} is each line's series color (1-based), fixed per target across charts
 */
record PercentileChart(String title, String subtitle, List<Line> lines) {

    /** A distribution in milliseconds. */
    record Line(String name, int slot, PercentileDistribution distribution) {
    }

    private static final int WIDTH = 860;
    private static final int HEIGHT = 440;
    private static final int LEFT = 74;
    /** The narrowest right margin; it widens to fit the longest end-of-line label. */
    private static final int MIN_RIGHT = 150;
    private static final int TOP = 96;
    private static final int BOTTOM = 48;
    /** The axis ends at 99.999%: a 30-second run at a few thousand requests per second has nothing beyond it. */
    static final int MAX_NINES = 5;
    private static final double MIN_LABEL_GAP = 14;

    PercentileChart {
        lines = List.copyOf(lines);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("No lines to draw");
        }
    }

    String toSvg() {
        double right = MIN_RIGHT;
        for (Line line : lines) {
            String label = line.name() + "  " + milliseconds(line.distribution().max());
            right = Math.max(right, 24 + Svg.approximateWidth(label, 11));
        }
        double plotWidth = WIDTH - LEFT - right;
        double plotHeight = HEIGHT - TOP - BOTTOM;
        double min = Double.MAX_VALUE;
        double max = 0;
        for (Line line : lines) {
            for (PercentileDistribution.Point point : visible(line.distribution())) {
                min = Math.min(min, point.value());
                max = Math.max(max, point.value());
            }
        }
        int lowDecade = (int) Math.floor(Math.log10(Math.max(min, 1e-3)));
        int highDecade = Math.max(lowDecade + 1, (int) Math.ceil(Math.log10(Math.max(max, 1e-3))));

        Svg svg = new Svg(WIDTH, HEIGHT, title);
        svg.text(20, 30, title, "text", 16, "start", true);
        svg.text(20, 50, subtitle, "muted", 12, "start", false);
        double legendX = 20;
        for (Line line : lines) {
            legendX = svg.legendEntry(legendX, 76, line.slot(), line.name());
        }

        double bottom = TOP + plotHeight;
        for (int decade = lowDecade; decade <= highDecade; decade++) {
            double y = y(Math.pow(10, decade), lowDecade, highDecade, plotHeight);
            svg.line(LEFT, y, LEFT + plotWidth, y, "grid", 1);
            svg.text(LEFT - 8, y + 4, milliseconds(Math.pow(10, decade)), "muted", 11, "end", false);
        }
        for (int nines = 0; nines <= MAX_NINES; nines++) {
            double x = LEFT + plotWidth * nines / MAX_NINES;
            svg.line(x, TOP, x, bottom, "grid", 1);
            svg.text(x, bottom + 18, percentileLabel(nines), "muted", 11, "middle", false);
        }
        svg.line(LEFT, bottom, LEFT + plotWidth, bottom, "axis", 1);
        svg.text(LEFT + plotWidth / 2, bottom + 38, "Percentile", "muted", 11, "middle", false);
        svg.raw("<text x=\"16\" y=\"" + Svg.n(TOP + plotHeight / 2) + "\" class=\"muted\" font-size=\"11\" "
                + "text-anchor=\"middle\" transform=\"rotate(-90 16 " + Svg.n(TOP + plotHeight / 2)
                + ")\">Latency (ms, log scale)</text>");

        List<EndLabel> labels = new ArrayList<>();
        for (Line line : lines) {
            StringBuilder points = new StringBuilder();
            double lastX = LEFT;
            double lastY = bottom;
            double lastValue = 0;
            for (PercentileDistribution.Point point : visible(line.distribution())) {
                lastX = LEFT + plotWidth * Math.min(nines(point.percentile()), MAX_NINES) / MAX_NINES;
                lastY = y(point.value(), lowDecade, highDecade, plotHeight);
                lastValue = point.value();
                points.append(Svg.n(lastX)).append(',').append(Svg.n(lastY)).append(' ');
            }
            String tooltip = String.format(Locale.ROOT, "%s: p50 %s, p99 %s, p99.9 %s, max %s", line.name(),
                    milliseconds(valueAt(line.distribution(), 0.5)), milliseconds(valueAt(line.distribution(), 0.99)),
                    milliseconds(valueAt(line.distribution(), 0.999)), milliseconds(line.distribution().max()));
            svg.raw("<polyline fill=\"none\" class=\"l" + line.slot() + "\" stroke-width=\"2\""
                    + " stroke-linejoin=\"round\" points=\"" + points.toString().strip() + "\"><title>"
                    + Svg.escape(tooltip) + "</title></polyline>");
            labels.add(new EndLabel(line, lastX, lastY, lastValue));
        }
        for (EndLabel label : spread(labels, TOP, bottom)) {
            svg.text(LEFT + plotWidth + 10, label.y() + 4, label.line().name() + "  " + milliseconds(label.value()),
                    "text", 11, "start", false);
            svg.raw("<circle cx=\"" + Svg.n(label.x()) + "\" cy=\"" + Svg.n(label.pointY()) + "\" r=\"3.5\" class=\"f"
                    + label.line().slot() + " ring\" stroke-width=\"2\"/>");
        }
        return svg.finish();
    }

    /** The points the axis can show: percentiles up to {@link #MAX_NINES} nines (the last one clamped onto it). */
    static List<PercentileDistribution.Point> visible(PercentileDistribution distribution) {
        List<PercentileDistribution.Point> visible = new ArrayList<>();
        for (PercentileDistribution.Point point : distribution.points()) {
            visible.add(point);
            if (nines(point.percentile()) >= MAX_NINES) {
                break;
            }
        }
        return visible;
    }

    /** {@code log10(1 / (1 - p))}: 0 for the median's left end, 1 for 90%, 2 for 99%, infinite for 100%. */
    static double nines(double percentile) {
        return percentile >= 1 ? Double.POSITIVE_INFINITY : -Math.log10(1 - percentile);
    }

    static double valueAt(PercentileDistribution distribution, double percentile) {
        for (PercentileDistribution.Point point : distribution.points()) {
            if (point.percentile() >= percentile) {
                return point.value();
            }
        }
        return distribution.max();
    }

    private static double y(double value, int lowDecade, int highDecade, double plotHeight) {
        double clamped = Math.max(value, Math.pow(10, lowDecade));
        double fraction = (Math.log10(clamped) - lowDecade) / (highDecade - lowDecade);
        return TOP + plotHeight * (1 - fraction);
    }

    static String percentileLabel(int nines) {
        if (nines == 0) {
            return "0%";
        }
        double percent = 100 * (1 - Math.pow(10, -nines));
        String text = String.format(Locale.ROOT, "%." + Math.max(0, nines - 2) + "f", percent);
        return text + "%";
    }

    static String milliseconds(double value) {
        if (value >= 100) {
            return String.format(Locale.ROOT, "%,.0f ms", value);
        }
        if (value >= 1) {
            return String.format(Locale.ROOT, "%.3g ms", value).replaceAll("\\.?0+ ms$", " ms");
        }
        return trimZeros(String.format(Locale.ROOT, "%.3f", value)) + " ms";
    }

    private static String trimZeros(String number) {
        String trimmed = number.replaceAll("0+$", "");
        return trimmed.endsWith(".") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }

    /** End-of-line labels pushed apart vertically so that none overlap, keeping their order. */
    static List<EndLabel> spread(List<EndLabel> labels, double top, double bottom) {
        List<EndLabel> sorted = new ArrayList<>(labels);
        sorted.sort(Comparator.comparingDouble(EndLabel::pointY));
        List<EndLabel> placed = new ArrayList<>();
        double previous = top - MIN_LABEL_GAP;
        for (EndLabel label : sorted) {
            double y = Math.max(label.pointY(), previous + MIN_LABEL_GAP);
            placed.add(label.at(y));
            previous = y;
        }
        double overflow = previous - bottom;
        if (overflow > 0) {
            List<EndLabel> shifted = new ArrayList<>();
            for (EndLabel label : placed) {
                shifted.add(label.at(label.y() - overflow));
            }
            placed = shifted;
        }
        return placed;
    }

    record EndLabel(Line line, double x, double pointY, double value, double y) {

        EndLabel(Line line, double x, double pointY, double value) {
            this(line, x, pointY, value, pointY);
        }

        EndLabel at(double newY) {
            return new EndLabel(line, x, pointY, value, newY);
        }
    }
}

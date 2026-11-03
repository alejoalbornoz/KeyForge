package io.keyforge.bench.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A horizontal grouped bar chart: one group per category (a target), one bar per series (a command) in each group, a
 * linear axis from zero, and every bar labeled with its value. A missing value (NaN) leaves its bar out.
 *
 * @param title      the headline
 * @param subtitle   one line under the title, for the setup
 * @param unit       the values' unit, shown in tooltips (for example {@code req/s})
 * @param categories the groups, top to bottom
 * @param series     the bars of each group, in legend order; each has one value per category
 */
record BarChart(String title, String subtitle, String unit, List<String> categories, List<Series> series) {

    record Series(String name, List<Double> values) {
    }

    private static final int WIDTH = 860;
    private static final int BAR = 14;
    private static final int BAR_GAP = 2;
    private static final int GROUP_GAP = 16;
    private static final int TOP = 96;
    private static final int BOTTOM = 40;
    private static final int RIGHT = 64;
    private static final double RADIUS = 4;

    BarChart {
        categories = List.copyOf(categories);
        series = List.copyOf(series);
        for (Series s : series) {
            if (s.values().size() != categories.size()) {
                throw new IllegalArgumentException("Series '" + s.name() + "' has " + s.values().size()
                        + " values for " + categories.size() + " categories");
            }
        }
        Svg.slot(series.size() - 1);
    }

    String toSvg() {
        double left = 24 + categories.stream().mapToDouble(c -> Svg.approximateWidth(c, 13)).max().orElse(0);
        double plotWidth = WIDTH - left - RIGHT;
        int groupHeight = series.size() * (BAR + BAR_GAP) - BAR_GAP;
        int plotHeight = categories.size() * (groupHeight + GROUP_GAP);
        int height = TOP + plotHeight + BOTTOM;
        double max = series.stream().flatMap(s -> s.values().stream()).filter(v -> !v.isNaN())
                .mapToDouble(Double::doubleValue).max().orElse(1);
        Axis axis = Axis.linear(max);

        Svg svg = new Svg(WIDTH, height, title);
        svg.text(20, 30, title, "text", 16, "start", true);
        svg.text(20, 50, subtitle, "muted", 12, "start", false);
        double legendX = 20;
        for (int i = 0; i < series.size(); i++) {
            legendX = svg.legendEntry(legendX, 76, Svg.slot(i), series.get(i).name());
        }

        double plotBottom = TOP + plotHeight - GROUP_GAP / 2.0;
        for (double tick : axis.ticks()) {
            double x = left + plotWidth * tick / axis.max();
            svg.line(x, TOP - 6, x, plotBottom, "grid", 1);
            svg.text(x, plotBottom + 18, Svg.compact(tick), "muted", 11, "middle", false);
        }
        svg.line(left, TOP - 6, left, plotBottom, "axis", 1);

        for (int c = 0; c < categories.size(); c++) {
            double groupTop = TOP + c * (groupHeight + GROUP_GAP);
            svg.text(left - 10, groupTop + groupHeight / 2.0 + 4.5, categories.get(c), "text", 13, "end", false);
            for (int s = 0; s < series.size(); s++) {
                double value = series.get(s).values().get(c);
                if (Double.isNaN(value)) {
                    continue;
                }
                double y = groupTop + s * (BAR + BAR_GAP);
                double width = plotWidth * value / axis.max();
                String tooltip = String.format(Locale.ROOT, "%s, %s: %,.0f %s", categories.get(c),
                        series.get(s).name(), value, unit);
                svg.raw("<path class=\"f" + Svg.slot(s) + "\" d=\"" + bar(left, y, width) + "\"><title>"
                        + Svg.escape(tooltip) + "</title></path>");
                svg.text(left + width + 5, y + BAR - 3, Svg.compact(value), "muted", 11, "start", false);
            }
        }
        return svg.finish();
    }

    /** A bar from the baseline {@code x0}, square there and rounded at its data end. */
    static String bar(double x0, double y, double width) {
        double x1 = x0 + width;
        if (width <= RADIUS) {
            return "M" + Svg.n(x0) + "," + Svg.n(y) + "H" + Svg.n(x1) + "V" + Svg.n(y + BAR) + "H" + Svg.n(x0) + "Z";
        }
        return "M" + Svg.n(x0) + "," + Svg.n(y)
                + "H" + Svg.n(x1 - RADIUS)
                + "Q" + Svg.n(x1) + "," + Svg.n(y) + " " + Svg.n(x1) + "," + Svg.n(y + RADIUS)
                + "V" + Svg.n(y + BAR - RADIUS)
                + "Q" + Svg.n(x1) + "," + Svg.n(y + BAR) + " " + Svg.n(x1 - RADIUS) + "," + Svg.n(y + BAR)
                + "H" + Svg.n(x0) + "Z";
    }

    /** A linear axis from zero to a round maximum at or above the data, with 4 to 6 evenly spaced ticks. */
    record Axis(double max, List<Double> ticks) {

        static Axis linear(double dataMax) {
            double rough = Math.max(dataMax, 1) / 5;
            double magnitude = Math.pow(10, Math.floor(Math.log10(rough)));
            double step = magnitude;
            for (double factor : new double[] {1, 2, 2.5, 5, 10}) {
                step = factor * magnitude;
                if (step >= rough) {
                    break;
                }
            }
            double max = Math.ceil(dataMax / step) * step;
            if (max <= 0) {
                max = step;
            }
            List<Double> ticks = new ArrayList<>();
            for (double tick = 0; tick <= max + step / 2; tick += step) {
                ticks.add(tick);
            }
            return new Axis(max, List.copyOf(ticks));
        }
    }
}

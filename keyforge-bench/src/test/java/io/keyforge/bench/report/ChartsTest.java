package io.keyforge.bench.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class ChartsTest {

    @Test
    void barChartsDrawOneLabeledBarPerValueWithLengthsProportionalToTheValues() throws Exception {
        BarChart chart = new BarChart("Throughput <P=1>", "50 clients & more", "req/s",
                List.of("Redis", "KeyForge"),
                List.of(new BarChart.Series("SET", List.of(200_000.0, 50_000.0)),
                        new BarChart.Series("GET", List.of(100_000.0, Double.NaN))));

        Document svg = parse(chart.toSvg());

        NodeList bars = svg.getElementsByTagName("path");
        assertThat(bars.getLength()).as("the NaN value has no bar").isEqualTo(3);
        List<Double> widths = new ArrayList<>();
        for (int i = 0; i < bars.getLength(); i++) {
            widths.add(width(((Element) bars.item(i)).getAttribute("d")));
        }
        assertThat(widths.get(0) / widths.get(1)).as("200k against 100k").isCloseTo(2.0, within(0.02));
        assertThat(widths.get(0) / widths.get(2)).as("200k against 50k").isCloseTo(4.0, within(0.04));
        assertThat(((Element) bars.item(0)).getAttribute("class")).isEqualTo("f1");
        assertThat(((Element) bars.item(1)).getAttribute("class")).isEqualTo("f2");
        assertThat(texts(svg)).contains("Throughput <P=1>", "50 clients & more", "Redis", "KeyForge", "SET", "GET",
                "200k", "100k", "50k");
        assertThat(bars.item(0).getTextContent()).isEqualTo("Redis, SET: 200,000 req/s");
    }

    @Test
    void barChartsRejectSeriesThatDoNotMatchTheCategoriesAndMoreSeriesThanColors() {
        assertThatThrownBy(() -> new BarChart("t", "s", "u", List.of("a", "b"),
                List.of(new BarChart.Series("x", List.of(1.0)))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 values for 2 categories");

        List<BarChart.Series> nine = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            nine.add(new BarChart.Series("s" + i, List.of(1.0)));
        }
        assertThatThrownBy(() -> new BarChart("t", "s", "u", List.of("a"), nine))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("series colors");
    }

    @Test
    void theLinearAxisEndsOnARoundNumberAtOrAboveTheData() {
        BarChart.Axis axis = BarChart.Axis.linear(183_456);

        assertThat(axis.max()).isEqualTo(200_000);
        assertThat(axis.ticks()).containsExactly(0.0, 50_000.0, 100_000.0, 150_000.0, 200_000.0);
        assertThat(BarChart.Axis.linear(1_000_000).max()).isEqualTo(1_000_000);
        assertThat(BarChart.Axis.linear(0).max()).isPositive();
    }

    @Test
    void percentileChartsSpaceTheNinesEvenlyAndStopAtFiveNines() throws Exception {
        assertThat(PercentileChart.nines(0)).isZero();
        assertThat(PercentileChart.nines(0.9)).isCloseTo(1, within(1e-9));
        assertThat(PercentileChart.nines(0.999)).isCloseTo(3, within(1e-9));
        assertThat(PercentileChart.nines(1.0)).isInfinite();
        assertThat(List.of(0, 1, 2, 3, 5).stream().map(PercentileChart::percentileLabel))
                .containsExactly("0%", "90%", "99%", "99.9%", "99.999%");

        PercentileDistribution distribution = new PercentileDistribution(List.of(
                new PercentileDistribution.Point(0.1, 0), new PercentileDistribution.Point(0.2, 0.5),
                new PercentileDistribution.Point(2, 0.99), new PercentileDistribution.Point(30, 0.999999),
                new PercentileDistribution.Point(40, 1.0)));
        assertThat(PercentileChart.visible(distribution)).extracting(PercentileDistribution.Point::value)
                .as("the first point past five nines ends the line").containsExactly(0.1, 0.2, 2.0, 30.0);

        PercentileChart chart = new PercentileChart("Latency", "50 connections",
                List.of(new PercentileChart.Line("Redis", 1, distribution),
                        new PercentileChart.Line("KeyForge", 3, distribution.scaled(10))));
        Document svg = parse(chart.toSvg());

        NodeList lines = svg.getElementsByTagName("polyline");
        assertThat(lines.getLength()).isEqualTo(2);
        assertThat(((Element) lines.item(1)).getAttribute("class")).isEqualTo("l3");
        assertThat(((Element) lines.item(0)).getAttribute("points").split(" ")).hasSize(4);
        assertThat(texts(svg)).contains("Latency", "Redis", "KeyForge", "0.1 ms", "1,000 ms", "99.999%",
                "Redis  30 ms", "KeyForge  300 ms");
    }

    @Test
    void endLabelsArePushedApartWithoutChangingTheirOrder() {
        PercentileChart.Line line = new PercentileChart.Line("x", 1,
                new PercentileDistribution(List.of(new PercentileDistribution.Point(1, 1))));
        List<PercentileChart.EndLabel> labels = List.of(
                new PercentileChart.EndLabel(line, 0, 100, 1), new PercentileChart.EndLabel(line, 0, 101, 2),
                new PercentileChart.EndLabel(line, 0, 300, 3), new PercentileChart.EndLabel(line, 0, 99, 4));

        List<PercentileChart.EndLabel> placed = PercentileChart.spread(labels, 50, 400);

        assertThat(placed).extracting(PercentileChart.EndLabel::value).containsExactly(4.0, 1.0, 2.0, 3.0);
        for (int i = 1; i < placed.size(); i++) {
            assertThat(placed.get(i).y() - placed.get(i - 1).y()).isGreaterThanOrEqualTo(14);
        }
        assertThat(placed.get(3).y()).as("a label with room stays at its line").isEqualTo(300);

        List<PercentileChart.EndLabel> crowded = PercentileChart.spread(List.of(
                new PercentileChart.EndLabel(line, 0, 399, 1), new PercentileChart.EndLabel(line, 0, 400, 2)),
                50, 400);
        assertThat(crowded.getLast().y()).as("labels past the bottom move up").isLessThanOrEqualTo(400);
    }

    @Test
    void numbersAreWrittenCompactlyAndTextIsEscaped() {
        assertThat(Svg.compact(950)).isEqualTo("950");
        assertThat(Svg.compact(1_234)).isEqualTo("1.2k");
        assertThat(Svg.compact(98_039)).isEqualTo("98k");
        assertThat(Svg.compact(1_500_000)).isEqualTo("1.5M");
        assertThat(Svg.compact(1_000_000)).isEqualTo("1M");
        assertThat(Svg.compact(12_345_678)).isEqualTo("12M");
        assertThat(Svg.n(12.0)).isEqualTo("12");
        assertThat(Svg.n(12.345)).isEqualTo("12.3");
        assertThat(Svg.escape("a<b & \"c\">")).isEqualTo("a&lt;b &amp; &quot;c&quot;&gt;");
        assertThat(PercentileChart.milliseconds(0.1234)).isEqualTo("0.123 ms");
        assertThat(PercentileChart.milliseconds(0.5)).isEqualTo("0.5 ms");
        assertThat(PercentileChart.milliseconds(12.34)).isEqualTo("12.3 ms");
        assertThat(PercentileChart.milliseconds(2)).isEqualTo("2 ms");
        assertThat(PercentileChart.milliseconds(1_234.5)).isEqualTo("1,235 ms");
    }

    @Test
    void everyChartCarriesItsOwnSurfaceAndADarkTheme() throws Exception {
        String svg = new BarChart("t", "s", "u", List.of("a"),
                List.of(new BarChart.Series("x", List.of(1.0)))).toSvg();

        assertThat(svg).contains("prefers-color-scheme: dark", "class=\"surface\"", ".f1{fill:#2a78d6}",
                ".f1{fill:#3987e5}");
        assertThat(parse(svg).getDocumentElement().getAttribute("role")).isEqualTo("img");
    }

    static Document parse(String svg) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        return factory.newDocumentBuilder().parse(new ByteArrayInputStream(svg.getBytes(StandardCharsets.UTF_8)));
    }

    private static List<String> texts(Document svg) {
        List<String> texts = new ArrayList<>();
        NodeList nodes = svg.getElementsByTagName("text");
        for (int i = 0; i < nodes.getLength(); i++) {
            texts.add(nodes.item(i).getTextContent());
        }
        return texts;
    }

    /** The width of a bar path, which starts {@code M x0,y H x1 ...}. */
    private static double width(String path) {
        String[] parts = path.substring(1).split("[,HQVZ ]");
        double x0 = Double.parseDouble(parts[0]);
        double end = Double.parseDouble(path.substring(path.indexOf('H') + 1).split("[QVZ]")[0]);
        return end - x0 + (path.contains("Q") ? 4 : 0);
    }
}

package io.keyforge.server.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.keyforge.raft.RaftStatus;
import io.keyforge.raft.Role;
import io.keyforge.server.command.ClientSession;
import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.command.CommandTable;
import io.keyforge.server.command.StandardCommands;
import io.keyforge.storage.InMemoryKeyValueStore;
import io.keyforge.storage.lsm.LsmStats;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * The Grafana dashboard ({@code deploy/grafana/dashboards/keyforge.json}) must be valid JSON, lay its panels out on
 * Grafana's 24-column grid, and query only metrics that a cluster node really exports, under the names the Prometheus
 * registry gives them. A renamed meter fails here instead of leaving an empty panel.
 */
class GrafanaDashboardTest {

    private static final Path DASHBOARD = Path.of("..", "deploy", "grafana", "dashboards", "keyforge.json");
    private static final Pattern METRIC = Pattern.compile("\\b((?:keyforge|jvm)_[a-z_]+)\\b");

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore(
            new InMemoryKeyValueStore.Options(16, Duration.ZERO, InstantSource.system()));
    private AutoCloseable jvm;

    @AfterEach
    void close() throws Exception {
        store.close();
        if (jvm != null) {
            jvm.close();
        }
    }

    @Test
    void everyQueriedMetricIsExportedByAClusterNode() throws Exception {
        String scrape = scrapeOfAClusterNode();
        Set<String> exported = new HashSet<>();
        for (String line : scrape.lines().toList()) {
            if (!line.startsWith("#") && !line.isBlank()) {
                exported.add(line.split("[{ ]", 2)[0]);
            }
        }

        Set<String> queried = new TreeSet<>();
        for (String expression : expressions()) {
            Matcher matcher = METRIC.matcher(expression);
            while (matcher.find()) {
                queried.add(matcher.group(1));
            }
        }

        assertThat(queried).as("the dashboard queries the Raft, LSM, command and JVM metrics").contains(
                "keyforge_raft_leader", "keyforge_raft_term", "keyforge_raft_commit_index", "keyforge_raft_apply_lag",
                "keyforge_lsm_compactions_total", "keyforge_commands_seconds_bucket", "jvm_gc_pause_seconds_sum");
        assertThat(exported).as("series in a cluster node's scrape").containsAll(queried);
    }

    @Test
    void theDashboardIsValidJsonWithUniquePanelsOnTheGrid() throws Exception {
        Map<?, ?> dashboard = (Map<?, ?>) Json.parse(Files.readString(DASHBOARD, StandardCharsets.UTF_8));

        assertThat(dashboard.get("uid")).isEqualTo("keyforge");
        List<Map<?, ?>> panels = panels(dashboard);
        assertThat(panels).hasSizeGreaterThan(10);
        Set<Object> ids = new HashSet<>();
        for (Map<?, ?> panel : panels) {
            assertThat(ids.add(panel.get("id"))).as("unique id %s", panel.get("id")).isTrue();
            assertThat((String) panel.get("title")).isNotBlank();
            Map<?, ?> grid = (Map<?, ?>) panel.get("gridPos");
            double x = ((Number) grid.get("x")).doubleValue();
            double width = ((Number) grid.get("w")).doubleValue();
            assertThat(x + width).as("panel '%s' fits the 24 columns", panel.get("title")).isLessThanOrEqualTo(24);
            if (!"row".equals(panel.get("type"))) {
                assertThat(((Map<?, ?>) panel.get("datasource")).get("uid")).isEqualTo("${datasource}");
                assertThat((List<?>) panel.get("targets")).isNotEmpty();
            }
        }
    }

    @Test
    void commandLatencyBucketsSpanMicrosecondsToSeconds() throws Exception {
        List<Double> bounds = new ArrayList<>();
        for (String line : scrapeOfAClusterNode().lines().toList()) {
            if (line.startsWith("keyforge_commands_seconds_bucket{command=\"get\"") && !line.contains("+Inf")) {
                Matcher le = Pattern.compile("le=\"([^\"]+)\"").matcher(line);
                assertThat(le.find()).isTrue();
                bounds.add(Double.parseDouble(le.group(1)));
            }
        }

        assertThat(bounds).hasSizeGreaterThan(20);
        assertThat(bounds.stream().mapToDouble(Double::doubleValue).min().orElseThrow()).isBetween(1e-5, 2e-5);
        assertThat(bounds.stream().mapToDouble(Double::doubleValue).max().orElseThrow()).isBetween(5.0, 10.0);
    }

    /** The meters {@code KeyForgeNode} registers for a cluster node, with made-up Raft and LSM state. */
    private String scrapeOfAClusterNode() throws InterruptedException {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().commonTags("node", "node-1");
        CommandTable commands = StandardCommands.create(store, InstantSource.system(), "test", Map.of());
        CommandDispatcher dispatcher = new CommandDispatcher(commands, null, new CommandMetrics(commands, registry));
        dispatcher.dispatch(new ClientSession(), new byte[][] {"GET".getBytes(StandardCharsets.UTF_8),
                "k".getBytes(StandardCharsets.UTF_8)});
        NodeMetrics.bindRaft(registry, () -> new RaftStatus("node-1", Role.LEADER, 2, "node-1", 10, 10, 10, 2, 0),
                () -> 10);
        NodeMetrics.bindLsm(registry, () -> new LsmStats(1, 100, 1, 100, 90, 1_000, 1, 90, 10, 0));
        jvm = NodeMetrics.bindJvm(registry);
        // GC pause timers appear with the first collection the JVM reports after binding.
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (registry.find("jvm.gc.pause").timer() == null && System.nanoTime() < deadline) {
            System.gc();
            Thread.sleep(50);
        }
        return registry.scrape();
    }

    private static List<String> expressions() throws Exception {
        Map<?, ?> dashboard = (Map<?, ?>) Json.parse(Files.readString(DASHBOARD, StandardCharsets.UTF_8));
        List<String> expressions = new ArrayList<>();
        for (Map<?, ?> panel : panels(dashboard)) {
            Object targets = panel.get("targets");
            if (targets instanceof List<?> list) {
                for (Object target : list) {
                    expressions.add((String) ((Map<?, ?>) target).get("expr"));
                }
            }
        }
        return expressions;
    }

    private static List<Map<?, ?>> panels(Map<?, ?> dashboard) {
        List<Map<?, ?>> panels = new ArrayList<>();
        for (Object panel : (List<?>) dashboard.get("panels")) {
            panels.add((Map<?, ?>) panel);
            if (((Map<?, ?>) panel).get("panels") instanceof List<?> nested) {
                nested.forEach(p -> panels.add((Map<?, ?>) p));
            }
        }
        return panels;
    }

    /** Just enough of a JSON parser for the dashboard: objects, arrays, strings, numbers, booleans and null. */
    static final class Json {
        private final String text;
        private int at;

        private Json(String text) {
            this.text = text;
        }

        static Object parse(String text) {
            Json json = new Json(text);
            Object value = json.value();
            json.skipWhitespace();
            if (json.at != text.length()) {
                throw json.error("trailing characters");
            }
            return value;
        }

        private Object value() {
            skipWhitespace();
            if (at >= text.length()) {
                throw error("unexpected end");
            }
            return switch (text.charAt(at)) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                default -> number();
            };
        }

        private Map<String, Object> object() {
            Map<String, Object> object = new LinkedHashMap<>();
            at++;
            skipWhitespace();
            if (peek() == '}') {
                at++;
                return object;
            }
            while (true) {
                skipWhitespace();
                String key = string();
                skipWhitespace();
                expect(':');
                if (object.put(key, value()) != null) {
                    throw error("duplicate key '" + key + "'");
                }
                skipWhitespace();
                if (peek() == ',') {
                    at++;
                } else {
                    expect('}');
                    return object;
                }
            }
        }

        private List<Object> array() {
            List<Object> array = new ArrayList<>();
            at++;
            skipWhitespace();
            if (peek() == ']') {
                at++;
                return array;
            }
            while (true) {
                array.add(value());
                skipWhitespace();
                if (peek() == ',') {
                    at++;
                } else {
                    expect(']');
                    return array;
                }
            }
        }

        private String string() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (true) {
                char c = text.charAt(at++);
                if (c == '"') {
                    return out.toString();
                }
                if (c == '\\') {
                    char escaped = text.charAt(at++);
                    switch (escaped) {
                        case '"', '\\', '/' -> out.append(escaped);
                        case 'n' -> out.append('\n');
                        case 't' -> out.append('\t');
                        case 'r' -> out.append('\r');
                        case 'b' -> out.append('\b');
                        case 'f' -> out.append('\f');
                        case 'u' -> {
                            out.append((char) Integer.parseInt(text.substring(at, at + 4), 16));
                            at += 4;
                        }
                        default -> throw error("bad escape \\" + escaped);
                    }
                } else if (c < 0x20) {
                    throw error("control character in a string");
                } else {
                    out.append(c);
                }
            }
        }

        private Double number() {
            int start = at;
            while (at < text.length() && "+-0123456789.eE".indexOf(text.charAt(at)) >= 0) {
                at++;
            }
            if (start == at) {
                throw error("unexpected '" + text.charAt(at) + "'");
            }
            return Double.parseDouble(text.substring(start, at));
        }

        private Object literal(String word, Object value) {
            if (!text.startsWith(word, at)) {
                throw error("expected " + word);
            }
            at += word.length();
            return value;
        }

        private void expect(char c) {
            if (peek() != c) {
                throw error("expected '" + c + "'");
            }
            at++;
        }

        private char peek() {
            if (at >= text.length()) {
                throw error("unexpected end");
            }
            return text.charAt(at);
        }

        private void skipWhitespace() {
            while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
                at++;
            }
        }

        private IllegalArgumentException error(String problem) {
            return new IllegalArgumentException("Invalid JSON at offset " + at + ": " + problem);
        }
    }
}

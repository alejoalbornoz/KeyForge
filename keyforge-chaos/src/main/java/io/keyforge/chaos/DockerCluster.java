package io.keyforge.chaos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ClusterControl} for the Docker Compose cluster in {@code keyforge-chaos/docker-compose.yml}, through the
 * {@code docker} command line (ADR 0016).
 *
 * <p>Node {@code kfN} is two containers: service {@code kfN} runs the KeyForge server, and {@code kfN-net} owns the
 * network namespace the server joins. Process faults signal the server container ({@code docker kill -s KILL}, {@code
 * STOP} or {@code CONT}, and {@code docker start}). Network faults run {@code iptables} and {@code toxiproxy-cli} in
 * the network container, which is never stopped, so its address never changes and faults outlive server restarts.
 * Commands for several nodes run in parallel: each {@code docker exec} takes a substantial fraction of a second.
 */
final class DockerCluster implements ClusterControl {

    private static final Logger log = LoggerFactory.getLogger(DockerCluster.class);

    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(60);

    /** Toxics on the {@code raft} proxy that {@code net-init} creates in every network container. */
    private static final List<String> LATENCY_TOXICS = List.of("latency-up", "latency-down");

    /**
     * @param server the server container's id
     * @param net    the network container's id
     * @param ip     the node's address on the cluster network
     */
    record Node(String name, String server, String net, String ip) {
    }

    private final Shell shell;
    private final Map<String, Node> nodes;
    private final String toolImage;

    /** @param toolImage an image with a shell, used to delete a stopped node's data through its volumes */
    DockerCluster(Shell shell, Map<String, Node> nodes, String toolImage) {
        this.shell = shell;
        this.nodes = new LinkedHashMap<>(nodes);
        this.toolImage = toolImage;
    }

    /** Finds the containers of {@code names} in a running Compose project, and their addresses. */
    static DockerCluster discover(Shell shell, String project, Collection<String> names) {
        Map<String, Node> nodes = new LinkedHashMap<>();
        String toolImage = null;
        for (String name : names) {
            String server = container(shell, project, name);
            String net = container(shell, project, name + "-net");
            String ip = check(shell.run(List.of("docker", "inspect", "-f",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", net), COMMAND_TIMEOUT),
                    "inspect " + name + "-net").strip();
            if (ip.isEmpty()) {
                throw new IllegalStateException(name + "-net has no address; is the cluster running?");
            }
            if (toolImage == null) {
                toolImage = check(shell.run(List.of("docker", "inspect", "-f", "{{.Config.Image}}", net),
                        COMMAND_TIMEOUT), "inspect " + name + "-net").strip();
            }
            nodes.put(name, new Node(name, server, net, ip));
        }
        return new DockerCluster(shell, nodes, toolImage);
    }

    Map<String, Node> nodes() {
        return Map.copyOf(nodes);
    }

    @Override
    public void partition(Map<String, Set<String>> grudges) {
        parallel(grudges.keySet(), name -> {
            List<String> rules = grudges.get(name).stream()
                    .map(peer -> "iptables -A INPUT -s " + node(peer).ip() + " -j DROP").toList();
            return rules.isEmpty() ? null : execInNet(name, String.join(" && ", rules));
        });
    }

    @Override
    public void delay(Collection<String> targets, int delayMillis, int jitterMillis) {
        String script = "toxiproxy-cli toxic add -t latency -n latency-up -u -a latency=" + delayMillis
                + " -a jitter=" + jitterMillis + " raft >/dev/null"
                + " && toxiproxy-cli toxic add -t latency -n latency-down -d -a latency=" + delayMillis
                + " -a jitter=" + jitterMillis + " raft >/dev/null";
        parallel(targets, name -> execInNet(name, script));
    }

    @Override
    public void drop(Collection<String> targets, int percent) {
        String probability = String.format(Locale.ROOT, "%.4f", percent / 100.0);
        parallel(targets, name -> execInNet(name, String.join(" && ", nodes.values().stream()
                .filter(peer -> !peer.name().equals(name))
                .map(peer -> "iptables -A INPUT -s " + peer.ip() + " -m statistic --mode random --probability "
                        + probability + " -j DROP")
                .toList())));
    }

    @Override
    public void healNetwork() {
        StringBuilder script = new StringBuilder("iptables -F INPUT");
        for (String toxic : LATENCY_TOXICS) {
            // Removing a toxic that is not there fails; that is fine.
            script.append(" && { toxiproxy-cli toxic remove -n ").append(toxic)
                    .append(" raft >/dev/null 2>&1; true; }");
        }
        parallel(nodes.keySet(), name -> execInNet(name, script.toString()));
    }

    @Override
    public void kill(Collection<String> targets) {
        signal(targets, "KILL");
    }

    @Override
    public void start(Collection<String> targets) {
        if (!targets.isEmpty()) {
            docker(withServers(List.of("docker", "start"), targets));
        }
    }

    @Override
    public void pause(Collection<String> targets) {
        signal(targets, "STOP");
    }

    @Override
    public void resume(Collection<String> targets) {
        signal(targets, "CONT");
    }

    @Override
    public void wipe(Collection<String> targets) {
        parallel(targets, name -> docker(List.of("docker", "run", "--rm", "--volumes-from", node(name).server(),
                "--entrypoint", "sh", toolImage, "-c", "find /data -mindepth 1 -delete")));
    }

    @Override
    public void healEverything(Collection<String> targets) {
        // Resume first: a paused process that is also asked to start stays paused.
        for (Runnable step : List.<Runnable>of(() -> resume(running(targets)), () -> start(targets),
                this::healNetwork)) {
            try {
                step.run();
            } catch (RuntimeException e) {
                log.warn("Healing step failed: {}", e.getMessage());
            }
        }
    }

    /** The targets whose server container is running (possibly paused): only those can be sent a signal. */
    private List<String> running(Collection<String> targets) {
        List<String> running = new ArrayList<>();
        for (String name : targets) {
            Shell.Result state = shell.run(List.of("docker", "inspect", "-f", "{{.State.Running}}",
                    node(name).server()), COMMAND_TIMEOUT);
            if (state.succeeded() && state.output().strip().equals("true")) {
                running.add(name);
            }
        }
        return running;
    }

    private void signal(Collection<String> targets, String signal) {
        if (!targets.isEmpty()) {
            docker(withServers(List.of("docker", "kill", "-s", signal), targets));
        }
    }

    private List<String> withServers(List<String> command, Collection<String> targets) {
        List<String> full = new ArrayList<>(command);
        targets.forEach(name -> full.add(node(name).server()));
        return full;
    }

    private String execInNet(String name, String script) {
        return docker(List.of("docker", "exec", node(name).net(), "sh", "-c", script));
    }

    private String docker(List<String> command) {
        return check(shell.run(command, COMMAND_TIMEOUT), String.join(" ", command));
    }

    private Node node(String name) {
        Node node = nodes.get(name);
        if (node == null) {
            throw new IllegalArgumentException("unknown node " + name + "; known: " + nodes.keySet());
        }
        return node;
    }

    /** Runs {@code action} for every name at once and waits for all; rethrows the first failure. */
    private void parallel(Collection<String> names, Function<String, String> action) {
        if (names.isEmpty()) {
            return;
        }
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<String>> results = names.stream().map(name -> executor.submit(() -> action.apply(name)))
                    .toList();
            RuntimeException failure = null;
            for (Future<String> result : results) {
                try {
                    result.get();
                } catch (ExecutionException e) {
                    RuntimeException cause = e.getCause() instanceof RuntimeException runtime ? runtime
                            : new IllegalStateException(e.getCause());
                    if (failure == null) {
                        failure = cause;
                    } else {
                        failure.addSuppressed(cause);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted", e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static String container(Shell shell, String project, String service) {
        String ids = check(shell.run(List.of("docker", "ps", "-a", "-q",
                "--filter", "label=com.docker.compose.project=" + project,
                "--filter", "label=com.docker.compose.service=" + service), COMMAND_TIMEOUT),
                "find the " + service + " container").strip();
        if (ids.isEmpty() || ids.contains("\n")) {
            throw new IllegalStateException("expected one container for service " + service + " in project "
                    + project + ", found " + (ids.isEmpty() ? "none" : ids.replace('\n', ' ')));
        }
        return ids;
    }

    private static String check(Shell.Result result, String what) {
        if (!result.succeeded()) {
            throw new IllegalStateException(what + " failed (exit " + result.exitCode() + "): " + result.output());
        }
        return result.output();
    }
}

package io.keyforge.chaos;

import java.time.Duration;

/**
 * Runs one linearizability test against a running Docker Compose cluster and writes its history. Usually started by
 * {@code keyforge-chaos/scripts/linearizability.sh}, which also starts the cluster and runs the checker.
 *
 * <p>Exit status: 0 when the history was written, 2 for bad options or a cluster that is not serving.
 */
public final class ChaosMain {

    private ChaosMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && (args[0].equals("--help") || args[0].equals("-h"))) {
            System.out.println("Usage: java -jar keyforge-chaos-all.jar [--name=value ...]\nOptions and defaults:");
            ChaosOptions.defaultValues().forEach((name, value) -> System.out.println("  --" + name + "="
                    + (value.isEmpty() ? "(random)" : value)));
            return;
        }
        ChaosOptions options;
        try {
            options = ChaosOptions.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }
        ClusterProbe probe = new ClusterProbe(options.nodes(), Duration.ofSeconds(2));
        try {
            ClusterControl control = options.nemesis().isEmpty() ? null
                    : DockerCluster.discover(Shell.processes(), options.project(), options.nodes().keySet());
            ChaosRun.Result result = new ChaosRun(options, control, probe).run();
            System.out.print(result.summary());
        } catch (IllegalStateException e) {
            System.err.println("The test could not run: " + e.getMessage());
            System.exit(2);
        }
    }
}

package io.keyforge.server;

import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Starts a single KeyForge node.
 *
 * <pre>
 * java -jar keyforge-server-all.jar [--config keyforge.properties] [--server.port=6380] ...
 * </pre>
 */
public final class KeyForgeMain {

    private static final Logger log = LoggerFactory.getLogger(KeyForgeMain.class);

    private KeyForgeMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        KeyForgeConfig config;
        try {
            config = KeyForgeConfig.fromCommandLine(args, System.getenv());
        } catch (IllegalArgumentException | UncheckedIOException e) {
            System.err.println("Invalid configuration: " + e.getMessage());
            System.exit(2);
            return;
        }
        MDC.put("nodeId", config.nodeId());
        log.info("Starting KeyForge {} with {}", KeyForgeNode.version(), config.toProperties());

        KeyForgeNode node;
        try {
            node = KeyForgeNode.start(config);
        } catch (RuntimeException e) {
            // For example a corrupt or locked data directory, or a port in use.
            log.error("Cannot start the node", e);
            System.exit(1);
            return;
        }
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().name("keyforge-shutdown").unstarted(() -> {
            MDC.put("nodeId", config.nodeId());
            log.info("Shutting down");
            node.close();
        }));
        log.info("Ready to accept connections on {}:{}", config.bindAddress(), node.port());
        // The server's event-loop threads (Netty) or accept thread (virtual threads) are non-daemon and keep the JVM
        // alive until the shutdown hook closes the node.
    }
}

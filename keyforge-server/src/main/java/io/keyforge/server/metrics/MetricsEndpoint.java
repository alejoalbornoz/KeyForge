package io.keyforge.server.metrics;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Serves a registry to Prometheus: {@code GET /metrics} answers with every meter in the Prometheus text format. It runs
 * on the JDK's built-in HTTP server, one virtual thread per request, on its own port: a scrape never touches the RESP
 * front end, and the gauges it reads are snapshots (ADR 0019).
 */
public final class MetricsEndpoint implements AutoCloseable {

    static final String PATH = "/metrics";
    static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private static final Logger log = LoggerFactory.getLogger(MetricsEndpoint.class);

    private final HttpServer server;
    private final ExecutorService executor;

    private MetricsEndpoint(HttpServer server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
    }

    /** Binds {@code bind:port} (0 picks a free port) and starts serving {@code registry}. */
    public static MetricsEndpoint start(PrometheusMeterRegistry registry, String bind, int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(bind, port), 0);
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("keyforge-metrics-", 0).factory());
        server.setExecutor(executor);
        server.createContext("/", exchange -> handle(exchange, registry));
        server.start();
        MetricsEndpoint endpoint = new MetricsEndpoint(server, executor);
        log.info("Serving metrics at http://{}:{}{}", bind, endpoint.port(), PATH);
        return endpoint;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private static void handle(HttpExchange exchange, PrometheusMeterRegistry registry) throws IOException {
        try (exchange) {
            if (!exchange.getRequestURI().getPath().equals(PATH)) {
                respond(exchange, 404, "Not found; metrics are at " + PATH + "\n");
            } else if (!exchange.getRequestMethod().equals("GET") && !exchange.getRequestMethod().equals("HEAD")) {
                exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                respond(exchange, 405, "Method not allowed\n");
            } else {
                exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
                byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
                if (exchange.getRequestMethod().equals("HEAD")) {
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    exchange.sendResponseHeaders(200, body.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(body);
                    }
                }
            }
        }
    }

    private static void respond(HttpExchange exchange, int status, String text) throws IOException {
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.close();
    }
}

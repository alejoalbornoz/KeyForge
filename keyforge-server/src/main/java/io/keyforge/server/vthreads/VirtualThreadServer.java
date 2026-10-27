package io.keyforge.server.vthreads;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import io.keyforge.protocol.RespRequestDecoder;
import io.keyforge.server.KeyForgeConfig;
import io.keyforge.server.KeyForgeServer;
import io.keyforge.server.command.CommandDispatcher;

/**
 * The virtual-thread front end: a {@link ServerSocket} accept loop on one platform thread, and one virtual thread per
 * connection doing blocking reads and writes ({@link Connection}). Commands run on the connection's virtual thread, as
 * they run on the event loop in the Netty server (ADR 0002). See
 * {@code docs/adr/0005-virtual-thread-server-and-pinning.md}.
 *
 * <p>The accept thread is a non-daemon platform thread, so it keeps the JVM alive until {@link #close()}; virtual
 * threads are always daemon threads.
 */
public final class VirtualThreadServer implements KeyForgeServer {

    private static final Logger log = LoggerFactory.getLogger(VirtualThreadServer.class);

    /** Same accept backlog as the Netty server. */
    private static final int BACKLOG = 1024;

    /** Receive buffer each connection starts with; see {@link ReceiveBuffer}. */
    static final int INITIAL_RECEIVE_BUFFER = 4 * 1024;

    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    private final KeyForgeConfig config;
    private final CommandDispatcher dispatcher;
    private final Set<Socket> openSockets = ConcurrentHashMap.newKeySet();

    private volatile boolean closed;
    private ServerSocket serverSocket;
    private ExecutorService connections;
    private Thread acceptThread;

    public VirtualThreadServer(KeyForgeConfig config, CommandDispatcher dispatcher) {
        this.config = config;
        this.dispatcher = dispatcher;
    }

    @Override
    public void start() {
        if (serverSocket != null || closed) {
            throw new IllegalStateException("Server already started or closed");
        }
        ServerSocket listener;
        try {
            listener = new ServerSocket();
            try {
                listener.bind(new InetSocketAddress(config.bindAddress(), config.port()), BACKLOG);
            } catch (IOException | RuntimeException e) {
                closeQuietly(listener);
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot listen on " + config.bindAddress() + ":" + config.port(), e);
        }
        serverSocket = listener;
        connections = Executors.newThreadPerTaskExecutor(withNodeId(Thread.ofVirtual().name("keyforge-conn-", 1)
                .factory()));
        acceptThread = withNodeId(Thread.ofPlatform().name("keyforge-accept").daemon(false).factory())
                .newThread(this::acceptLoop);
        acceptThread.start();
        log.debug("Virtual-thread server bound to {}", listener.getLocalSocketAddress());
    }

    @Override
    public int port() {
        if (serverSocket == null) {
            throw new IllegalStateException("Server not started");
        }
        return serverSocket.getLocalPort();
    }

    @Override
    public void close() {
        closed = true;
        if (serverSocket != null) {
            closeQuietly(serverSocket);
        }
        boolean interrupted = false;
        if (acceptThread != null) {
            interrupted = joinUninterruptibly(acceptThread);
        }
        // The accept loop has ended, so no socket is added from here on.
        for (Socket socket : openSockets) {
            closeQuietly(socket);
        }
        if (connections != null) {
            connections.shutdown();
            try {
                if (!connections.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.warn("Connection threads still running {} s after close", CLOSE_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void acceptLoop() {
        while (!closed) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (closed) {
                    return;
                }
                // For example out of file descriptors. Pause so that a persistent failure does not spin a core.
                log.warn("Accept failed: {}", e.toString());
                pauseAfterAcceptFailure();
                continue;
            }
            dispatchConnection(socket);
        }
    }

    private void dispatchConnection(Socket socket) {
        openSockets.add(socket);
        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            connections.execute(() -> serve(socket));
        } catch (IOException | RejectedExecutionException e) {
            log.debug("Dropping connection {}: {}", socket.getRemoteSocketAddress(), e.toString());
            openSockets.remove(socket);
            closeQuietly(socket);
        }
    }

    /** Body of a connection's virtual thread. */
    private void serve(Socket socket) {
        try (socket) {
            Connection connection = new Connection(dispatcher,
                    new RespRequestDecoder(config.maxBulkLength(), config.maxArrayLength()), INITIAL_RECEIVE_BUFFER);
            connection.serve(socket.getInputStream(), socket.getOutputStream());
        } catch (IOException e) {
            log.debug("Connection {} failed: {}", socket.getRemoteSocketAddress(), e.toString());
        } catch (RuntimeException e) {
            log.warn("Unexpected error on connection {}", socket.getRemoteSocketAddress(), e);
        } finally {
            openSockets.remove(socket);
        }
    }

    /** Threads, virtual ones included, carry the node id in their MDC, so their log lines name the node. */
    private ThreadFactory withNodeId(ThreadFactory delegate) {
        String nodeId = config.nodeId();
        return task -> delegate.newThread(() -> {
            MDC.put("nodeId", nodeId);
            task.run();
        });
    }

    private static void pauseAfterAcceptFailure() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Returns whether the calling thread was interrupted while waiting. */
    private static boolean joinUninterruptibly(Thread thread) {
        boolean interrupted = false;
        while (true) {
            try {
                thread.join();
                return interrupted;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
    }

    private static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            log.debug("Ignoring failure to close {}: {}", closeable, e.toString());
        }
    }
}

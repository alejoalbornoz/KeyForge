package io.keyforge.bench;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Generates RESP load against a server and reports throughput, latency percentiles (HdrHistogram, corrected for
 * coordinated omission in open loop; see {@link LoadConnection}) and, given the server's pid, its memory, threads and
 * CPU.
 *
 * <p>A run: wait for the server, {@code SET} every key, open all connections, let them sit idle for
 * {@code settle-seconds} and sample the server, run traffic for {@code warmup-seconds} (discarded), then measure for
 * {@code duration-seconds}.
 *
 * <pre>
 * java -jar keyforge-bench-all.jar --host=server --connections=10000 --rate=50000 --label=netty --output=results.csv
 * </pre>
 */
public final class LoadGenerator {

    private static final int PREPOPULATE_BATCH = 1_000;
    private static final int CONNECT_ATTEMPTS = 3;
    private static final double NANOS_PER_MICRO = 1_000.0;
    private static final double BYTES_PER_MIB = 1024.0 * 1024.0;

    private final BenchOptions options;
    private final PrintStream progress;

    public LoadGenerator(BenchOptions options, PrintStream progress) {
        this.options = options;
        this.progress = progress;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && (args[0].equals("--help") || args[0].equals("-h"))) {
            System.out.println("Options (--name=value) and defaults:");
            BenchOptions.defaultValues().forEach((name, value) -> System.out.println("  --" + name + "=" + value));
            return;
        }
        BenchOptions options;
        try {
            options = BenchOptions.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }
        LoadGenerator generator = new LoadGenerator(options, System.err);
        Measurement measurement = generator.run();
        System.out.print(measurement.result().summary());
        if (options.output() != null) {
            measurement.result().appendCsv(options.output());
            measurement.writePercentiles(percentilesFile(options));
        }
    }

    /** Runs the benchmark described by the options. */
    public Measurement run() throws IOException, InterruptedException {
        ProcessStats server = options.serverPid() > 0 ? ProcessStats.of(options.serverPid()) : null;
        if (server != null && !server.available()) {
            throw new IllegalStateException("Cannot read /proc/" + options.serverPid()
                    + "; run in a container started with --pid container:<server>");
        }
        ProcessStats self = ProcessStats.self();
        RequestTemplates templates = new RequestTemplates(options.keyspace(), options.valueSize(), options.getRatio());
        awaitServer(Duration.ofSeconds(60));
        if (options.prepopulate()) {
            prepopulate(templates);
        }
        ProcessStats.Sample base = sample(server);
        double heapBase = liveHeapMib();

        boolean epoll = useEpoll();
        IoHandlerFactory ioHandler = epoll ? EpollIoHandler.newFactory() : NioIoHandler.newFactory();
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(
                options.threads(), new DefaultThreadFactory("bench-io"), ioHandler);
        Run run = new Run(options, templates);
        try {
            long connectStart = System.nanoTime();
            List<LoadConnection> connections = connectAll(group, epoll, run);
            double connectSeconds = (System.nanoTime() - connectStart) / 1e9;
            progress.printf(Locale.ROOT, "[%s] %d/%d connections in %.1f s%n",
                    options.label(), connections.size(), options.connections(), connectSeconds);
            if (connections.isEmpty()) {
                throw new IOException("No connection could be established: " + run.failure.get());
            }
            Thread.sleep(options.settle().toMillis());
            ProcessStats.Sample idle = sample(server);
            double heapIdle = liveHeapMib();

            startTraffic(connections, run);
            report("warmup", run, options.warmup(), server);
            run.latency.getIntervalHistogram(); // discard the warm-up
            run.timerLag.getIntervalHistogram();

            Histogram latency = new Histogram(3);
            Histogram timerLag = new Histogram(3);
            long errorsBefore = run.errors.sum();
            long disconnectsBefore = run.disconnects.sum();
            ProcessStats.Sample serverStart = sample(server);
            ProcessStats.Sample selfStart = sample(self.available() ? self : null);
            long measureStart = System.nanoTime();
            Peak peak = new Peak();
            for (int second = 1; second <= options.duration().toSeconds(); second++) {
                sleepUntil(measureStart + TimeUnit.SECONDS.toNanos(second));
                Histogram interval = run.latency.getIntervalHistogram();
                latency.add(interval);
                timerLag.add(run.timerLag.getIntervalHistogram());
                ProcessStats.Sample now = sample(server);
                peak.update(now);
                printProgress("measure", second, interval, now);
            }
            long measureNanos = System.nanoTime() - measureStart;
            ProcessStats.Sample serverEnd = sample(server);
            ProcessStats.Sample selfEnd = sample(self.available() ? self : null);
            run.running = false;
            peak.update(serverEnd);

            RunResult result = new RunResult(
                    options.label(), epoll ? "epoll" : "nio", options.connections(), connections.size(),
                    connectSeconds, options.openLoop(), options.rate(), latency.getTotalCount() / (measureNanos / 1e9),
                    latency.getTotalCount(), run.errors.sum() - errorsBefore, run.disconnects.sum() - disconnectsBefore,
                    measureNanos / 1e9,
                    micros(latency.getValueAtPercentile(50)), micros(latency.getValueAtPercentile(90)),
                    micros(latency.getValueAtPercentile(99)), micros(latency.getValueAtPercentile(99.9)),
                    micros(latency.getValueAtPercentile(99.99)), micros(latency.getMaxValue()),
                    latency.getMean() / NANOS_PER_MICRO,
                    micros(timerLag.getValueAtPercentile(99)), micros(timerLag.getMaxValue()),
                    mib(base), mib(idle), server == null ? -1 : peak.rssBytes / BYTES_PER_MIB, heapBase, heapIdle,
                    threads(idle), server == null ? -1 : peak.threads,
                    server == null ? -1 : serverEnd.coresSince(serverStart, measureNanos),
                    selfEnd == null ? -1 : selfEnd.coresSince(selfStart, measureNanos));
            return new Measurement(result, latency);
        } finally {
            run.running = false;
            group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    /** The server's live heap after a full GC, or -1 without a server pid or if {@code jcmd} cannot attach. */
    private double liveHeapMib() throws InterruptedException {
        if (options.serverPid() == 0) {
            return -1;
        }
        try {
            return ServerHeap.liveBytesAfterFullGc(options.serverPid()) / BYTES_PER_MIB;
        } catch (IOException | IllegalArgumentException e) {
            progress.println("[" + options.label() + "] cannot measure the server heap: " + e.getMessage());
            return -1;
        }
    }

    private boolean useEpoll() {
        return switch (options.transport()) {
            case "epoll" -> {
                Epoll.ensureAvailability();
                yield true;
            }
            case "nio" -> false;
            default -> Epoll.isAvailable();
        };
    }

    private void awaitServer(Duration timeout) throws IOException, InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(options.host(), options.port()), 1_000);
                return;
            } catch (IOException e) {
                if (System.nanoTime() > deadline) {
                    throw new IOException("Server " + options.host() + ":" + options.port() + " not reachable", e);
                }
                Thread.sleep(200);
            }
        }
    }

    /** {@code SET}s every key over one pipelining connection, so that {@code GET}s find values. */
    private void prepopulate(RequestTemplates templates) throws IOException {
        byte[] ok = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(options.host(), options.port()), 5_000);
            socket.setSoTimeout(30_000);
            OutputStream out = socket.getOutputStream();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            for (int start = 0; start < templates.keyspace(); start += PREPOPULATE_BATCH) {
                int end = Math.min(templates.keyspace(), start + PREPOPULATE_BATCH);
                ByteArrayOutputStream batch = new ByteArrayOutputStream();
                for (int i = start; i < end; i++) {
                    batch.writeBytes(templates.set(i));
                }
                batch.writeTo(out);
                byte[] replies = new byte[(end - start) * ok.length];
                in.readFully(replies);
                for (int i = 0; i < replies.length; i += ok.length) {
                    if (!Arrays.equals(replies, i, i + ok.length, ok, 0, ok.length)) {
                        throw new IOException("Unexpected reply while prepopulating");
                    }
                }
            }
        }
    }

    private List<LoadConnection> connectAll(EventLoopGroup group, boolean epoll, Run run)
            throws InterruptedException {
        AtomicLong seeds = new AtomicLong(new SplittableRandom().nextLong());
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(epoll ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30_000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline().addLast(new LoadConnection(run, seeds.incrementAndGet()));
                    }
                })
                .remoteAddress(options.host(), options.port());
        Semaphore permits = new Semaphore(options.connectConcurrency());
        List<LoadConnection> connected = Collections.synchronizedList(new ArrayList<>(options.connections()));
        AtomicInteger failed = new AtomicInteger();
        for (int i = 0; i < options.connections(); i++) {
            permits.acquire();
            connect(bootstrap, 1, permits, connected, failed, run);
        }
        permits.acquire(options.connectConcurrency());
        if (failed.get() > 0) {
            progress.printf("[%s] %d connections failed; first failure: %s%n",
                    options.label(), failed.get(), run.failure.get());
        }
        return new ArrayList<>(connected);
    }

    private void connect(Bootstrap bootstrap, int attempt, Semaphore permits, List<LoadConnection> connected,
            AtomicInteger failed, Run run) {
        ChannelFuture future = bootstrap.connect();
        future.addListener(done -> {
            if (done.isSuccess()) {
                connected.add(future.channel().pipeline().get(LoadConnection.class));
                permits.release();
            } else if (attempt < CONNECT_ATTEMPTS) {
                future.channel().eventLoop().schedule(
                        () -> connect(bootstrap, attempt + 1, permits, connected, failed, run),
                        100L * attempt, TimeUnit.MILLISECONDS);
            } else {
                run.failure.compareAndSet(null, done.cause().toString());
                failed.incrementAndGet();
                permits.release();
            }
        });
    }

    /** Starts every connection; in open loop the first requests are spread evenly over one interval. */
    private void startTraffic(List<LoadConnection> connections, Run run) {
        run.running = true;
        long start = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
        SplittableRandom random = new SplittableRandom();
        for (LoadConnection connection : connections) {
            long firstDue = options.openLoop() ? start + random.nextLong(options.intervalNanos()) : start;
            Channel channel = connection.channel();
            channel.eventLoop().execute(() -> connection.start(firstDue));
        }
    }

    private void report(String phase, Run run, Duration length, ProcessStats server)
            throws IOException, InterruptedException {
        long start = System.nanoTime();
        for (int second = 1; second <= length.toSeconds(); second++) {
            sleepUntil(start + TimeUnit.SECONDS.toNanos(second));
            // Consumes each interval; the warm-up's latencies are only printed, never kept.
            printProgress(phase, second, run.latency.getIntervalHistogram(), sample(server));
        }
    }

    private void printProgress(String phase, int second, Histogram interval, ProcessStats.Sample server) {
        StringBuilder line = new StringBuilder(String.format(Locale.ROOT,
                "[%s] %s %3ds  %8d req/s  p50 %7.0f us  p99 %8.0f us  p99.9 %8.0f us",
                options.label(), phase, second, interval.getTotalCount(),
                micros(interval.getValueAtPercentile(50)), micros(interval.getValueAtPercentile(99)),
                micros(interval.getValueAtPercentile(99.9))));
        if (server != null) {
            line.append(String.format(Locale.ROOT, "  | server RSS %5.0f MiB, %d threads",
                    server.rssBytes() / BYTES_PER_MIB, server.threads()));
        }
        progress.println(line);
    }

    private static ProcessStats.Sample sample(ProcessStats stats) throws IOException {
        return stats == null ? null : stats.sample();
    }

    private static void sleepUntil(long deadlineNanos) throws InterruptedException {
        long remaining;
        while ((remaining = deadlineNanos - System.nanoTime()) > 0) {
            TimeUnit.NANOSECONDS.sleep(remaining);
        }
    }

    private static double micros(long nanos) {
        return nanos / NANOS_PER_MICRO;
    }

    private static double mib(ProcessStats.Sample sample) {
        return sample == null ? -1 : sample.rssBytes() / BYTES_PER_MIB;
    }

    private static int threads(ProcessStats.Sample sample) {
        return sample == null ? -1 : sample.threads();
    }

    private static Path percentilesFile(BenchOptions options) {
        String name = String.format(Locale.ROOT, "%s-%dc-%s.hgrm", options.label(), options.connections(),
                options.openLoop() ? options.rate() + "rps" : "max");
        Path parent = options.output().toAbsolutePath().getParent();
        return parent == null ? Path.of(name) : parent.resolve(name);
    }

    /** The result of a run plus its full latency histogram. */
    public record Measurement(RunResult result, Histogram latency) {

        /** Writes the latency distribution in HdrHistogram's percentile format, in microseconds. */
        void writePercentiles(Path file) throws IOException {
            try (PrintStream out = new PrintStream(Files.newOutputStream(file), false, StandardCharsets.UTF_8)) {
                latency.outputPercentileDistribution(out, NANOS_PER_MICRO);
            }
        }
    }

    /** State shared by every connection of a run. */
    static final class Run {
        private final boolean openLoop;
        private final long intervalNanos;
        private final RequestTemplates templates;
        /** Latencies in nanoseconds; written by every event loop, read by the reporting thread. */
        final Recorder latency = new Recorder(3);
        final Recorder timerLag = new Recorder(3);
        final LongAdder errors = new LongAdder();
        final LongAdder disconnects = new LongAdder();
        final AtomicReference<String> failure = new AtomicReference<>();
        volatile boolean running;

        Run(BenchOptions options, RequestTemplates templates) {
            this.openLoop = options.openLoop();
            this.intervalNanos = options.intervalNanos();
            this.templates = templates;
        }

        boolean running() {
            return running;
        }

        boolean openLoop() {
            return openLoop;
        }

        long intervalNanos() {
            return intervalNanos;
        }

        RequestTemplates templates() {
            return templates;
        }

        void recordLatency(long nanos, boolean error) {
            latency.recordValue(Math.max(0, nanos));
            if (error) {
                errors.increment();
            }
        }

        void recordTimerLag(long nanos) {
            timerLag.recordValue(Math.max(0, nanos));
        }

        void recordDisconnect() {
            disconnects.increment();
        }

        void recordFailure(Throwable cause) {
            failure.compareAndSet(null, cause.toString());
        }
    }

    /** Highest server RSS and thread count seen. */
    private static final class Peak {
        long rssBytes;
        int threads;

        void update(ProcessStats.Sample sample) {
            if (sample != null) {
                rssBytes = Math.max(rssBytes, sample.rssBytes());
                threads = Math.max(threads, sample.threads());
            }
        }
    }
}

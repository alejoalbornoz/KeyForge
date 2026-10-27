package io.keyforge.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import io.keyforge.protocol.RespRequestDecoder;
import io.keyforge.server.KeyForgeConfig;
import io.keyforge.server.KeyForgeServer;
import io.keyforge.server.command.CommandDispatcher;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * The Netty front end. Pipeline per connection:
 * {@link RespRequestFrameDecoder} &rarr; {@link CommandChannelHandler} &rarr; {@link RespReplyEncoder}.
 * Commands run on the connection's event loop; see {@code docs/adr/0002-threading-model-and-in-memory-store.md}.
 */
public final class NettyServer implements KeyForgeServer {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    /** Netty's defaults, stated explicitly because backpressure depends on them. */
    private static final WriteBufferWaterMark WATER_MARK = new WriteBufferWaterMark(32 * 1024, 64 * 1024);

    private final KeyForgeConfig config;
    private final CommandDispatcher dispatcher;

    private EventLoopGroup acceptGroup;
    private EventLoopGroup ioGroup;
    private Channel serverChannel;

    public NettyServer(KeyForgeConfig config, CommandDispatcher dispatcher) {
        this.config = config;
        this.dispatcher = dispatcher;
    }

    @Override
    public void start() throws InterruptedException {
        acceptGroup = new MultiThreadIoEventLoopGroup(1, threadFactory("keyforge-accept"), NioIoHandler.newFactory());
        ioGroup = new MultiThreadIoEventLoopGroup(
                config.ioThreads(), threadFactory("keyforge-io"), NioIoHandler.newFactory());
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(acceptGroup, ioGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, WATER_MARK)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            RespRequestDecoder decoder =
                                    new RespRequestDecoder(config.maxBulkLength(), config.maxArrayLength());
                            channel.pipeline()
                                    .addLast("decoder", new RespRequestFrameDecoder(decoder))
                                    .addLast("encoder", new RespReplyEncoder())
                                    .addLast("commands", new CommandChannelHandler(dispatcher));
                        }
                    });
            serverChannel = bootstrap.bind(config.bindAddress(), config.port()).sync().channel();
        } catch (InterruptedException | RuntimeException e) {
            close();
            throw e;
        }
        log.debug("Netty server bound to {}", serverChannel.localAddress());
    }

    @Override
    public int port() {
        if (serverChannel == null) {
            throw new IllegalStateException("Server not started");
        }
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public void close() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (acceptGroup != null) {
            acceptGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (ioGroup != null) {
            ioGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    /** Event-loop threads carry the node id in their MDC, so every log line they produce names the node. */
    private ThreadFactory threadFactory(String prefix) {
        ThreadFactory delegate = new DefaultThreadFactory(prefix);
        String nodeId = config.nodeId();
        return task -> delegate.newThread(() -> {
            MDC.put("nodeId", nodeId);
            task.run();
        });
    }
}

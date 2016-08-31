package com.cpm;


import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.*;

//TODO: Clean this up
public class ClientBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ClientBuilder.class);

    private NioEventLoopGroup group;

    public Client connect(InetSocketAddress address, int connectTimeoutMilliseconds, int pendingRequestLimit,
                          int heartBeatIntervalSeconds) {
        group = createDefaultGroup();
        return connect(group, address, connectTimeoutMilliseconds, pendingRequestLimit, heartBeatIntervalSeconds);
    }

    public Client connect(EventLoopGroup group, InetSocketAddress address, int connectTimeoutMilliseconds,
                          int pendingRequestLimit, int heartBeatIntervalSeconds) {
        try {
            logger.debug("enter connect");
            CompletableFuture<Client> cf = connectAsync(group, address, connectTimeoutMilliseconds,
                    pendingRequestLimit, heartBeatIntervalSeconds, Runnables.doNothing());
            logger.debug("waiting on connection");
            //TODO: Review this
            return Uninterruptibles.getUninterruptibly(cf);
        } catch (ExecutionException e) {
            throw new ClientException(e.getMessage());
        }
    }

    public CompletableFuture<Client> connectAsync(EventLoopGroup group, InetSocketAddress address,
                                                  int connectTimeoutMilliseconds, int pendingRequestLimit,
                                                  int heartBeatIntervalSeconds, Runnable closeAction) {
        logger.debug("enter connectAsync");
        CompletableFuture<Client> result = new CompletableFuture<>();

        Bootstrap b = createBootstrap(group, address, connectTimeoutMilliseconds);
        logger.debug("connectAsync - connect");
        ChannelFuture cf = b.connect();
        cf.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                result.complete(new DefaultClient(future.channel(), address, pendingRequestLimit,
                        heartBeatIntervalSeconds, closeAction));
            } else {
                result.completeExceptionally(future.cause());
            }
        });

        logger.debug("exit connectAsync");
        return result;
    }

    public void shutdown() {
        if(null != group) {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    private Bootstrap createBootstrap(EventLoopGroup group, InetSocketAddress address, int connectTimeoutMilliseconds) {
        logger.debug("enter createBootstrap");
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMilliseconds)
                .remoteAddress(address)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new FrameEncoder());
                        p.addLast(new FrameDecoder());
                    }
                });
        logger.debug("exit createBootstrap");
        return b;
    }

    private static ThreadFactory threadFactory(String name) {
        return new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
    }

    /**
     * Passing 0 as thread count tells Netty to use the default thread count, which uses system property
     * io.netty.eventLoopThreads if set or if not set uses number of processors * 2. You can see this in
     * https://github.com/netty/netty/blob/4.1/transport/src/main/java/io/netty/channel/MultithreadEventLoopGroup.java.
     */
    private static NioEventLoopGroup createDefaultGroup() {
        return new NioEventLoopGroup(0, threadFactory("nio-worker"));
    }
}

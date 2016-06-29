package com.cpm;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.*;

public class ClientBuilder {

    /**
     * Passing 0 as thread count tells Netty to use the default thread count, which uses system property
     * io.netty.eventLoopThreads if set or if not set uses number of processors * 2. You can see this in
     * https://github.com/netty/netty/blob/4.1/transport/src/main/java/io/netty/channel/MultithreadEventLoopGroup.java.
     */
    private final EventLoopGroup group = new NioEventLoopGroup(0, threadFactory("nio-worker"));

    //TODO: Add setting for connection timeout
    public Client connect(InetSocketAddress address, int heartBeatIntervalSeconds) {
        try {
            return Uninterruptibles.getUninterruptibly(connectAsync(address, heartBeatIntervalSeconds));
        } catch (ExecutionException e) {
            throw new ClientException(e.getMessage());
        }
    }

    public CompletableFuture<Client> connectAsync(InetSocketAddress address, int heartBeatIntervalSeconds) {
        CompletableFuture<Client> result = new CompletableFuture<>();

        Bootstrap b = createBootstrap(address);
        ChannelFuture cf = b.connect();
        cf.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                result.complete(new Client(future.channel(), address, heartBeatIntervalSeconds));
            } else {
                result.completeExceptionally(future.cause());
            }
        });

        return result;
    }

    public void shutdown() {
        group.shutdownGracefully().syncUninterruptibly();
    }

    private Bootstrap createBootstrap(InetSocketAddress address) {
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(address)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new FrameEncoder());
                        p.addLast(new FrameDecoder());
                    }
                });
        return b;
    }

    private ThreadFactory threadFactory(String name) {
        return new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
    }
}

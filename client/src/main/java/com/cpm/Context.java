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

//TODO: Consider renaming
public class Context {

    private EventLoopGroup group = new NioEventLoopGroup(0, threadFactory("nio-worker"));

    //TODO: Add outstanding request limit
    //TODO: Move to client?
    private final ConcurrentMap<Integer, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();

    //TODO: Add setting for connection timeout
    public Client connect(InetSocketAddress address) {
        try {
            return Uninterruptibles.getUninterruptibly(connectAsync(address));
        } catch (ExecutionException e) {
            throw new ClientException(e.getMessage());
        }
    }

    public CompletableFuture<Client> connectAsync(InetSocketAddress address) {
        CompletableFuture<Client> result = new CompletableFuture<>();

        Bootstrap b = createBootstrap(address);
        ChannelFuture cf = b.connect();
        cf.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                result.complete(new Client(future.channel(), address, pending));
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
                        ch.pipeline().addLast(new FrameEncoder());
                        ch.pipeline().addLast(new FrameDecoder());
                        //TODO: Move to client and load in ctor?
                        p.addLast(new ClientConnectionHandler(pending));
                    }
                });
        return b;
    }

    private ThreadFactory threadFactory(String name) {
        return new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
    }
}

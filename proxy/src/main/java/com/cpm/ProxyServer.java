package com.cpm;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import java.net.InetSocketAddress;

public class ProxyServer {
    private int port;

    public ProxyServer(int port) {
        this.port = port;
    }

    public void run() throws Exception {
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);

        InetSocketAddress remoteAddressA = new InetSocketAddress("localhost", 8080);
        InetSocketAddress remoteAddressB = new InetSocketAddress("localhost", 8081);

        //TODO: Cleanup
        ClientBuilder builder = new ClientBuilder();

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new FrameEncoder());
                            ch.pipeline().addLast(new FrameDecoder());
                            ch.pipeline().addLast(new FrameMergeHandler(builder, remoteAddressA, remoteAddressB));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 8082;
        }
        new ProxyServer(port).run();
    }
}

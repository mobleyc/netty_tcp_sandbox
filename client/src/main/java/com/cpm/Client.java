package com.cpm;


import io.netty.channel.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.netty.handler.timeout.IdleState.ALL_IDLE;

public class Client {

    private final Channel channel;
    private final InetSocketAddress address;
    //TODO: Add outstanding request limit
    private final ConcurrentMap<Integer, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();

    public Client(Channel channel, InetSocketAddress address, int heartBeatIntervalSeconds) {
        this.channel = channel;
        this.address = address;

        ChannelPipeline p = channel.pipeline();
        p.addLast("idleStateHandler", new IdleStateHandler(0, 0, heartBeatIntervalSeconds));
        p.addLast(new Client.ClientConnectionHandler(pending));

        channel.closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            System.out.println("Connection closed");
            clearPending(new ConnectionException(address, "Connection closed."));
        });
    }

    public CompletableFuture<Frame> send(Frame query) {

        if (!channel.isWritable()) {
            throw new ConnectionException(address, "Attempted to write to a closed connection");
        }

        CompletableFuture<Frame> outboundF = new CompletableFuture<>();
        CompletableFuture<Frame> old = pending.put(query.getStreamId(), outboundF);
        if (null != old) {
            throw new IllegalStateException("Unexpected state. A pending request was overwritten by a " +
                    "new request. Stream Id: " + query.getStreamId());
        }

        channel.writeAndFlush(query).addListener((ChannelFutureListener) writeFuture -> {
            if (!writeFuture.isSuccess()) {
                //TODO: Replace with logging
                System.out.println("Client write failed");
                clearPending(new ConnectionException(address, "Attempted to write to a closed connection",
                        writeFuture.cause()));
                close();
            }
        });

        return outboundF;
    }

    public void close() {
        channel.close().syncUninterruptibly();
    }

    private void clearPending(ClientException exception) {
        //TODO: Replace with logging
        System.out.println("Enter clearPending. Total pending: " + pending.size());

        Iterator<CompletableFuture<Frame>> it = pending.values().iterator();
        while (it.hasNext()) {
            CompletableFuture<Frame> pendingF = it.next();
            pendingF.completeExceptionally(exception);
            it.remove();
        }
    }

    class ClientConnectionHandler extends ChannelInboundHandlerAdapter {
        //TODO: Add metrics
        private final ConcurrentMap<Integer, CompletableFuture<Frame>> pending;

        public ClientConnectionHandler(ConcurrentMap<Integer, CompletableFuture<Frame>> pending) {
            this.pending = pending;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            Frame inbound = (Frame) msg;

            CompletableFuture<Frame> inboundF = pending.remove(inbound.getStreamId());
            if (null == inboundF) {
                //TODO: Revisit and add logging. Determine if connection should remain open?
                throw new RuntimeException("Not Implemented yet. Received message for unknown stream id.");
            }

            inboundF.complete(inbound);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            //TODO: Review. Should connection close when an exception is raised?
            cause.printStackTrace();
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            boolean isAllIdle = evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == ALL_IDLE;
            if (!isAllIdle) {
                System.out.println("Event received and skipped: " + evt);
                return;
            }

            boolean isWriteable = ctx.channel().isWritable();
            if (!isWriteable) {
                System.out.println("Received idle state event, but connection is closed. Heartbeat message will not be sent.");
            }

            System.out.println("Sending heartbeat message");
            send(Frame.PING);
        }
    }
}

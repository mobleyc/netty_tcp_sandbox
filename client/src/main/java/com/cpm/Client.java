package com.cpm;


import io.netty.channel.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.netty.handler.timeout.IdleState.ALL_IDLE;

public class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private final Channel channel;
    private final InetSocketAddress address;
    private final int pendingRequestLimit;

    //Can be updated by IO thread (e.g. in Future callback handlers) or by client thread
    private volatile ClientException closeReason = null;

    //TODO: Review concurrencyLevel configuration
    private final ConcurrentMap<Integer, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();

    public Client(Channel channel, InetSocketAddress address, int pendingRequestLimit, int heartBeatIntervalSeconds) {
        this.channel = channel;
        this.address = address;
        this.pendingRequestLimit = pendingRequestLimit;

        ChannelPipeline p = channel.pipeline();
        p.addLast("idleStateHandler", new IdleStateHandler(0, 0, heartBeatIntervalSeconds));
        p.addLast(new Client.ClientConnectionHandler(pending));

        channel.closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            logger.debug("Connection closed");
            if (null == closeReason) {
                closeReason = new ConnectionException(address, "Connection closed.");
            }
            clearPending(closeReason);
        });
    }

    public CompletableFuture<Frame> send(Frame query) {

        if (!channel.isWritable()) {
            throw new ConnectionException(address, "Attempted to write to a closed connection");
        }

        CompletableFuture<Frame> outboundF = new CompletableFuture<>();
        CompletableFuture<Frame> old = pending.put(query.getStreamId(), outboundF);
        if (null != old) {
            logger.debug("Unexpected state on pending.put");
            closeReason = new ClientException(new IllegalStateException("Unexpected state. A pending request was " +
                    "overwritten by a new request. Stream Id: " + query.getStreamId()));
            close();
        }

        //TODO: Use AtomicInteger. ConcurrentHashMap.size() is transient and not synchronized.
        //    ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html
        if (pending.size() >= pendingRequestLimit) {
            logger.debug("Max limit reached");
            closeReason = new ClientException("Max pending request limit reached.");
            close();
        }

        channel.writeAndFlush(query).addListener((ChannelFutureListener) writeFuture -> {
            if (!writeFuture.isSuccess()) {
                logger.debug("Client write failed");
                closeReason = new ConnectionException(address, "Attempted to write to a closed connection",
                        writeFuture.cause());
                close();
            }
        });

        return outboundF;
    }

    public void close() {
        channel.close().syncUninterruptibly();
    }

    private void clearPending(ClientException exception) {
        logger.debug("Enter clearPending. Total pending: " + pending.size());

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
                throw new RuntimeException("Not Implemented yet. Received message for unknown stream id.");
            }

            inboundF.complete(inbound);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            boolean isAllIdle = evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == ALL_IDLE;
            if (!isAllIdle) {
                logger.debug("Event received and skipped: " + evt);
                return;
            }

            boolean isWriteable = ctx.channel().isWritable();
            if (!isWriteable) {
                logger.debug("Received idle state event, but connection is closed. Heartbeat message will not be sent.");
            }

            logger.debug("Sending heartbeat message");
            send(Frame.PING);
        }
    }
}

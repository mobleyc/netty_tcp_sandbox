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
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.handler.timeout.IdleState.ALL_IDLE;

/**
 * This class can be called by both the client thread pool and the Netty IO thread pool. In general
 * the Netty IO thread pool calls the Future listener code blocks.
 */
public class DefaultClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(DefaultClient.class);

    private static final ClientException MAX_PENDING_EXCEPTION =
            new ClientException("Max pending request limit reached.");

    private final Channel channel;
    private final InetSocketAddress address;
    private final int pendingRequestLimit;
    private final Runnable closeAction;

    //Can be updated by IO thread (e.g. in Future callback handlers) or by client thread
    private volatile ClientException closeReason = null;

    //ConcurrentHashMap.size() is transient and not synchronized.
    //    ref: ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html
    private final AtomicInteger pendingRequestCounter = new AtomicInteger();

    //TODO: Review concurrencyLevel configuration
    private final ConcurrentMap<Integer, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();

    public DefaultClient(Channel channel, InetSocketAddress address, int pendingRequestLimit, Runnable closeAction) {
        this(channel, address, pendingRequestLimit, 0, closeAction);
    }

    public DefaultClient(Channel channel, InetSocketAddress address, int pendingRequestLimit,
                         int heartBeatIntervalSeconds, Runnable closeAction) {
        this.channel = channel;
        this.address = address;
        this.pendingRequestLimit = pendingRequestLimit;
        this.closeAction = closeAction;

        ChannelPipeline p = channel.pipeline();
        if(0 != heartBeatIntervalSeconds) {
            p.addLast("idleStateHandler", new IdleStateHandler(0, 0, heartBeatIntervalSeconds));
        }
        p.addLast(new DefaultClient.ClientConnectionHandler(pending));

        channel.closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            logger.debug("Connection closed");
            if (null == closeReason) {
                closeReason = new ConnectionException(address, "Connection closed.");
            }
            clearPending(closeReason);
            closeAction.run();
        });
    }

    @Override
    public CompletableFuture<Frame> send(Frame query) {

        if (!channel.isWritable()) {
            //pending future's should already be drained at this point
            throw new ConnectionException(address, "Attempted to write to a closed connection");
        }


        if (!tryIncrementPending()) {
            logger.debug("Max limit reached");
            closeReason = MAX_PENDING_EXCEPTION;
            close();

            throw MAX_PENDING_EXCEPTION;
        }

        CompletableFuture<Frame> outboundF = new CompletableFuture<>();
        CompletableFuture<Frame> old = pending.put(query.getStreamId(), outboundF);
        if (null != old) {
            logger.debug("Unexpected state on pending.put");
            closeReason = new ClientException(new IllegalStateException("Unexpected state. A pending request was " +
                    "overwritten by a new request. Stream Id: " + query.getStreamId()));
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

    private boolean tryIncrementPending() {
        int pending;
        do {
            pending = pendingRequestCounter.get();
            if (pending >= pendingRequestLimit) {
                return false;
            }
        } while (!pendingRequestCounter.compareAndSet(pending, pending + 1));
        return true;
    }

    @Override
    public boolean isActive() {
        return channel.isActive();
    }

    @Override
    public void close() {
        channel.close().syncUninterruptibly();
    }

    private void clearPending(ClientException exception) {
        logger.debug("Enter clearPending. Total pending: " + pendingRequestCounter.get());

        Iterator<CompletableFuture<Frame>> it = pending.values().iterator();
        while (it.hasNext()) {
            CompletableFuture<Frame> pendingF = it.next();
            pendingF.completeExceptionally(exception);
            it.remove();
        }

        pendingRequestCounter.set(0);
    }

    /**
     * This class is called by the Netty IO thread pool. One instance is created per Channel.
     */
    private class ClientConnectionHandler extends ChannelInboundHandlerAdapter {
        //TODO: Add metrics
        private final ConcurrentMap<Integer, CompletableFuture<Frame>> pending;

        ClientConnectionHandler(ConcurrentMap<Integer, CompletableFuture<Frame>> pending) {
            this.pending = pending;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            Frame inbound = (Frame) msg;

            CompletableFuture<Frame> inboundF = pending.remove(inbound.getStreamId());
            if (null == inboundF) {
                closeReason = new ClientException(new IllegalStateException("Received message for unknown stream id. " +
                        "Stream Id: " + inbound.getStreamId()));
                close();
                return;
            }

            pendingRequestCounter.decrementAndGet();

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

            boolean isWritable = ctx.channel().isWritable();
            if (!isWritable) {
                logger.debug("Received idle state event, but connection is closed. Heartbeat message will not be sent.");
                return;
            }

            logger.debug("Sending heartbeat message");
            send(Frame.PING);
        }
    }
}

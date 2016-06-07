package com.cpm;


import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

/**
 * There is one ClientConnectionHandler per Channel/Connection
 */
public class ClientConnectionHandler extends ChannelInboundHandlerAdapter {

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
}

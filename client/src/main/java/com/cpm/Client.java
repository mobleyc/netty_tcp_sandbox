package com.cpm;


import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

//TODO: Add heartbeats
public class Client {

    private final Channel channel;
    private final ConcurrentMap<Integer, CompletableFuture<Frame>> pending;

    public Client(Channel channel, ConcurrentMap<Integer, CompletableFuture<Frame>> pending) {
        this.channel = channel;
        this.pending = pending;

        channel.closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            errorOutPending(new ClientException("Connection being closed.", channelFuture.cause()));
        });
    }

    public CompletableFuture<Frame> send(Frame query) {

        CompletableFuture<Frame> outboundF = new CompletableFuture<>();
        if(!channel.isWritable()) {
            outboundF.completeExceptionally(new ClientException("Channel is not in a writeable state. This usually " +
                    "indicates the channel was closed."));
            return outboundF;
        }

        CompletableFuture<Frame> old = pending.put(query.getStreamId(), outboundF);
        if (null != old) {
            throw new IllegalStateException("Unexpected state. A pending request was overwritten by a " +
                    "new request. Stream Id: " + query.getStreamId());
        }

        channel.writeAndFlush(query).addListener((ChannelFutureListener) writeFuture -> {
            if (!writeFuture.isSuccess()) {
                errorOutPending(new ClientException("Error sending message to server.", writeFuture.cause()));
                close();
            }
        });

        return outboundF;
    }

    public void close() {
        channel.close().syncUninterruptibly();
    }

    private void errorOutPending(ClientException exception) {
        Iterator<CompletableFuture<Frame>> it = pending.values().iterator();
        while (it.hasNext()) {
            CompletableFuture<Frame> pendingF = it.next();
            pendingF.completeExceptionally(exception);
            it.remove();
        }
    }
}

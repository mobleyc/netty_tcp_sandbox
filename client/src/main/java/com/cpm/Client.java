package com.cpm;


import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

//TODO: Add heartbeats
public class Client {

    private final Channel channel;
    private final InetSocketAddress address;
    private final ConcurrentMap<Integer, CompletableFuture<Frame>> pending;

    public Client(Channel channel, InetSocketAddress address, ConcurrentMap<Integer, CompletableFuture<Frame>> pending) {
        this.channel = channel;
        this.address = address;
        this.pending = pending;

        channel.closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            System.out.println("Connection closed");
            errorOutPending(new ConnectionException(address, "Connection closed."));
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
                errorOutPending(new ConnectionException(address, "Attempted to write to a closed connection",
                        writeFuture.cause()));
                close();
            }
        });

        return outboundF;
    }

    public void close() {
        channel.close().syncUninterruptibly();
    }

    private void errorOutPending(ClientException exception) {
        //TODO: Replace with logging
        System.out.println("Enter errorOutPending. Total pending: " + pending.size());

        Iterator<CompletableFuture<Frame>> it = pending.values().iterator();
        while (it.hasNext()) {
            CompletableFuture<Frame> pendingF = it.next();
            pendingF.completeExceptionally(exception);
            it.remove();
        }
    }
}

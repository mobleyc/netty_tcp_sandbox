package com.cpm;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FrameMergeHandler extends SimpleChannelInboundHandler<Frame> {

    private static final Logger logger = LoggerFactory.getLogger(FrameMergeHandler.class);

    private final ClientBuilder builder;
    private final InetSocketAddress remoteHostA;
    private final InetSocketAddress remoteHostB;

    private Client clientA;
    private Client clientB;

    public FrameMergeHandler(ClientBuilder builder, InetSocketAddress remoteHostA, InetSocketAddress remoteHostB) {
        this.builder = builder;
        this.remoteHostA = remoteHostA;
        this.remoteHostB = remoteHostB;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Proxy channelActive");

        builder.connectAsync(ctx.channel().eventLoop(), remoteHostA,
                5000 /*Connection timeout in millis*/,
                1000 /*max pending limit*/,
                0 /*disable heartbeat*/,
                () -> close(clientB))
                .thenAccept(c -> clientA = c);

        builder.connectAsync(ctx.channel().eventLoop(), remoteHostB,
                5000 /*Connection timeout in millis*/,
                1000 /*max pending limit*/,
                0 /*disable heartbeat*/,
                () -> close(clientA))
                .thenAccept(c -> clientB = c);

        logger.debug("Proxy connections initialized.");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Frame frame) throws Exception {
        logger.debug("Proxy channelRead0");
        if ((null == clientA) || (null == clientB)) {
            logger.debug("Skipping read. Clients not initialized yet");
            return;
        }

        CompletableFuture<Frame> frameA = clientA.send(frame);
        CompletableFuture<Frame> frameB = clientB.send(frame);
        CompletableFuture.allOf(frameA, frameB).thenRun(() -> {
            Frame fa = null;
            Frame fb = null;
            try {
                fa = frameA.get();
                fb = frameB.get();
                Frame f = new Frame(FrameType.REPLY, fa.getStreamId(), fa.getPayload() + ":" + fb.getPayload());
                logger.debug("Sending merged : " + f);
                channelHandlerContext.writeAndFlush(f);
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error in proxy: ", e);
            }
        });
    }

    private void close(Client client) {
        if ((null != client) && client.isActive()) {
            client.close();
        }
    }
}

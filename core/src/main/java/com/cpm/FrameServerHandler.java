package com.cpm;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert a Frame to a Response
 */
public class FrameServerHandler extends SimpleChannelInboundHandler<Frame> {

    private static final Logger logger = LoggerFactory.getLogger(FrameServerHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Frame request) throws Exception {
        logger.debug("Server received frame: " + request);
        if(request.getType() != FrameType.PING) {
            channelHandlerContext.writeAndFlush(new Frame(FrameType.REPLY, request.getStreamId(), "test reply"));
        } else {
            channelHandlerContext.writeAndFlush(Frame.PING_ACK);
        }
    }
}

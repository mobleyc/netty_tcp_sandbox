package com.cpm;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Convert a Frame to a Response
 */
public class FrameServerHandler extends SimpleChannelInboundHandler<Frame> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Frame request) throws Exception {
        channelHandlerContext.writeAndFlush(new Frame(FrameType.REPLY, 0, "test"));
    }
}

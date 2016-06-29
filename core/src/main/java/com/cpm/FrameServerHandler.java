package com.cpm;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Convert a Frame to a Response
 */
public class FrameServerHandler extends SimpleChannelInboundHandler<Frame> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Frame request) throws Exception {
        System.out.println("Server received frame: " + request);
        if(request.getType() != FrameType.PING) {
            channelHandlerContext.writeAndFlush(new Frame(FrameType.REPLY, request.getStreamId(), "test reply"));
        } else {
            channelHandlerContext.writeAndFlush(Frame.PING_ACK);
        }
    }
}

package com.cpm;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/**
 * Converts a Response to a byte stream
 */
public class FrameEncoder extends MessageToByteEncoder<Frame> {

    @Override
    protected void encode(ChannelHandlerContext context, Frame response, ByteBuf byteBuf)
            throws Exception {

        int messageLength = response.getPayload().length();
        byteBuf.writeShort(messageLength);
        byteBuf.writeByte(response.getType());
        byteBuf.writeShort(response.getStreamId());
        byteBuf.writeBytes(response.getPayload().getBytes(StandardCharsets.US_ASCII));
    }
}

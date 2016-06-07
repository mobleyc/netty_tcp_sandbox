package com.cpm;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.charset.StandardCharsets;

/**
 * Converts a stream of bytes to a Frame.
 */
public class FrameDecoder extends LengthFieldBasedFrameDecoder {

    private static int LENGTH_FIELD_OFFSET = 0;
    private static int LENGTH_FIELD_LENGTH = 2;
    private static int TOTAL_HEADER_LENGTH = 5;
    private static int INITIAL_BYTES_TO_STRIP = 2;

    public FrameDecoder() {
        super(Frame.MAX_MESSAGE_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH,
                TOTAL_HEADER_LENGTH - LENGTH_FIELD_LENGTH, INITIAL_BYTES_TO_STRIP);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        byte frameType = frame.readByte();
        int streamId = frame.readUnsignedShort();
        String payload = frame.toString(StandardCharsets.US_ASCII);

        return new Frame(frameType, streamId, payload);
    }
}

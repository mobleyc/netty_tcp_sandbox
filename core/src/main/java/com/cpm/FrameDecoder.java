package com.cpm;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.charset.StandardCharsets;

/**
 * Converts a stream of bytes to a Frame.
 *
 * Frame:
 *
 * All frames begin with a fixed 3-octet header followed by a variable length payload.
 *
 * +--------------------------+
 * | Length (16)              |
 * +-------------+------------+
 * | Type (8)    |
 * +-------------+-------------------------------------------------+
 * | Payload (0...)                                                |
 * +---------------------------------------------------------------+
 *
 * The fields of the frame header are defined as follows:
 *   1. Length - The length of the frame payload expressed as an unsigned 16-bit integer. The value of this field
 *               does not include the header length.
 *   2. Type   - The 8-bit type of the frame. The frame type determines the format and semantics
 *               of the frame.
 */
public class FrameDecoder extends LengthFieldBasedFrameDecoder {

    private static int LENGTH_FIELD_OFFSET = 0;
    private static int LENGTH_FIELD_LENGTH = 2;
    private static int TOTAL_HEADER_LENGTH = 3;
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
        String payload = frame.toString(StandardCharsets.US_ASCII);

        return new Frame(frameType, payload);
    }
}

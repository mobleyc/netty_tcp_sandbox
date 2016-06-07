package com.cpm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


import static org.junit.Assert.*;

public class FrameEncoderTest {

    private EmbeddedChannel channel;

    @Before
    public void setup() throws Exception {
        channel = new EmbeddedChannel(new FrameEncoder());
    }

    @After
    public void teardown() throws Exception {
        assertFalse(channel.finish());
    }

    @Test
    public void Should_encode_response() {
        Frame r = new Frame(FrameType.REQUEST, 1, "test");
        channel.writeOutbound(r);

        ByteBuf byteBuf = (ByteBuf) channel.readOutbound();
        assertNotNull(byteBuf);

        ByteBuf aggregatedBuffer = Unpooled.buffer();
        aggregatedBuffer.writeBytes(byteBuf);
        aggregatedBuffer.resetReaderIndex();

        assertEquals("000400000174657374",
                ByteBufUtil.hexDump(aggregatedBuffer));
    }

    @Test
    @Ignore
    public void Test_encode() {
        Frame r = new Frame(FrameType.REQUEST, 1, "test");
        channel.writeOutbound(r);

        ByteBuf byteBuf = (ByteBuf) channel.readOutbound();
        assertNotNull(byteBuf);

        ByteBuf aggregatedBuffer = Unpooled.buffer();
        aggregatedBuffer.writeBytes(byteBuf);
        aggregatedBuffer.resetReaderIndex();

        System.out.println(ByteBufUtil.hexDump(aggregatedBuffer));
    }
}
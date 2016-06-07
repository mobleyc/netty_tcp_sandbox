package com.cpm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assert.*;

public class FrameDecoderTest {

    private EmbeddedChannel channel;

    @Before
    public void setup() throws Exception {
        channel = new EmbeddedChannel(new FrameDecoder());
    }

    @After
    public void teardown() throws Exception {
        assertFalse(channel.finish());
    }

    @Test
    public void Should_read_single_frame() {
        ByteBuf incoming = Unpooled.buffer();

        incoming.writeBytes(DatatypeConverter.parseHexBinary("000400000174657374"));
        channel.writeInbound(incoming);

        Frame r = (Frame) channel.readInbound();
        assertEquals(FrameType.REQUEST, r.getType());
        assertEquals(1, r.getStreamId());
        assertEquals("test", r.getPayload());

        assertNull(channel.readInbound());
    }

    @Test
    public void Should_read_single_frame_from_partial_writes() {
        ByteBuf incoming1 = Unpooled.buffer();
        // length=4, type=0x0
        incoming1.writeBytes(DatatypeConverter.parseHexBinary("000400"));
        channel.writeInbound(incoming1);

        ByteBuf incoming2 = Unpooled.buffer();
        // payload=test
        incoming2.writeBytes(DatatypeConverter.parseHexBinary("000174657374"));
        channel.writeInbound(incoming2);

        Frame r = (Frame) channel.readInbound();
        assertEquals(FrameType.REQUEST, r.getType());
        assertEquals("test", r.getPayload());

        assertNull(channel.readInbound());
    }

    @Test
    public void Should_read_two_frames() {
        ByteBuf incoming = Unpooled.buffer();
        // two of the same messages:
        //     length=4, type=0x0 payload=test
        incoming.writeBytes(
                DatatypeConverter.parseHexBinary("000400000174657374000400000174657374"));
        channel.writeInbound(incoming);

        Frame r = (Frame) channel.readInbound();
        assertEquals(FrameType.REQUEST, r.getType());
        assertEquals("test", r.getPayload());

        assertNotNull(channel.readInbound());
        assertNull(channel.readInbound());
    }
}
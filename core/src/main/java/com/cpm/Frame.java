package com.cpm;

public class Frame {

    public static final Frame PING = new Frame(FrameType.PING, 0, "ping");
    //TODO: It would be better to add a flags field to frame header and use that for PING ACK
    public static final Frame PING_ACK = new Frame(FrameType.PING, 0, "ping ack");

    public static final short LENGTH_SIZE = 2;
    public static final short STREAM_ID_SIZE = 2;
    public static final short TYPE_SIZE = 1;
    public static final short HEADER_SIZE = LENGTH_SIZE + TYPE_SIZE + STREAM_ID_SIZE;
    public static final short MAX_MESSAGE_LENGTH = 256;

    private byte type;
    private final int streamId;
    private String payload;

    public Frame(byte type, int streamId, String payload) {
        this.type = type;
        this.streamId = streamId;
        this.payload = payload;
    }

    public byte getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public int getStreamId() {
        return streamId;
    }

    @Override
    public String toString() {
        return "Frame{" +
                "type=" + type +
                ", streamId=" + streamId +
                '}';
    }
}

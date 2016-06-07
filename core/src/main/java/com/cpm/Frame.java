package com.cpm;

public class Frame {

    public static final short LENGTH_SIZE = 2;
    public static final short TYPE_SIZE = 1;
    public static final short HEADER_SIZE = LENGTH_SIZE + TYPE_SIZE;
    public static final short MAX_MESSAGE_LENGTH = 256;

    private byte type;
    private int streamId;
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
}

package com.cpm;

public class Frame {

    public static final short LENGTH_SIZE = 2;
    public static final short TYPE_SIZE = 1;
    public static final short HEADER_SIZE = LENGTH_SIZE + TYPE_SIZE;
    public static final short MAX_MESSAGE_LENGTH = 256;

    private byte type;
    private String payload;

    public Frame(byte type, String payload) {
        this.type = type;
        this.payload = payload;
    }

    public byte getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }
}

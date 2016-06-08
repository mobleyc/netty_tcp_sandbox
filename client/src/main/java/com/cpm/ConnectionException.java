package com.cpm;


import java.net.InetAddress;
import java.net.InetSocketAddress;

public class ConnectionException extends ClientException {

    public final InetSocketAddress address;
    private static final long serialVersionUID = 0;

    public ConnectionException(InetSocketAddress address, String msg) {
        super(msg);
        this.address = address;
    }

    public ConnectionException(InetSocketAddress address, String msg, Throwable cause) {
        super(msg, cause);
        this.address = address;
    }

    public InetAddress getHost() {
        return address.getAddress();
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public String getMessage() {
        return String.format("[%s] %s", getHost(), super.getMessage());
    }
}

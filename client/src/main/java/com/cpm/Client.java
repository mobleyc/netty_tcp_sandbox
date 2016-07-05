package com.cpm;

import java.util.concurrent.CompletableFuture;


public interface Client {

    CompletableFuture<Frame> send(Frame query);

    boolean isActive();

    void close();
}

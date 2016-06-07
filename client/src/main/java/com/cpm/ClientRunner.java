package com.cpm;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientRunner {

    // TODO: Test max message length
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(
                    "Usage: " + ClientRunner.class.getSimpleName() + " <host> <port>");
            return;
        }

        final String host = args[0];
        final int port = Integer.parseInt(args[1]);

        Context ctx = new Context();
        Client client = null;
        try {
            client = ctx.connect(new InetSocketAddress(host, port));
            runSend(client, 1);
        } finally {
            if (null != client) {
                client.close();
                client.close();
            }
            ctx.shutdown();
        }
    }

    private static void runSend(Client client, int numberOfTimes) {
        List<CompletableFuture<Frame>> buffer = new ArrayList<>();
        for(int i = 0; i < numberOfTimes; i++ ) {
            System.out.println("Sending: " + i);

            buffer.add(client.send(new Frame(FrameType.REQUEST, i, "test request")));
        }

        for(CompletableFuture<Frame> f: buffer) {
            try {
                Frame response = f.get(3, TimeUnit.SECONDS);
                System.out.println("Received frame: " + response);
                System.out.println("Received frame payload: " + response.getPayload());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
}

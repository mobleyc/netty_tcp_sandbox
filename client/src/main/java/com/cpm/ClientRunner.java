package com.cpm;

import com.google.common.base.Stopwatch;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

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

        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());

        final String host = args[0];
        final int port = Integer.parseInt(args[1]);


        ClientBuilder builder = new ClientBuilder();
        Client client = null;

        Stopwatch watch = Stopwatch.createStarted();
        try {
            client = builder.connect(new InetSocketAddress(host, port),
                    5000 /*Connection timeout in millis*/,
                    1000 /*max pending limit*/,
                    20   /*heartbeat seconds*/);
            watch.stop();
            System.out.println("Connected. Time to connect: " + watch.elapsed(TimeUnit.SECONDS) + " second(s)");
            Thread.sleep(TimeUnit.SECONDS.toMillis(65));

            //runSend(client, 10);
        } finally {
            if(watch.isRunning()) {
                watch.stop();
                System.out.println("Time wait to connect: " + watch.elapsed(TimeUnit.SECONDS) + " second(s)");
            }

            if (null != client) {
                client.close();
                client.close();
            }
            builder.shutdown();
        }
    }

    private static void runSend(Client client, int numberOfTimes) throws InterruptedException {
        List<CompletableFuture<Frame>> buffer = new ArrayList<>();
        for (int i = 0; i < numberOfTimes; i++) {
            buffer.add(client.send(new Frame(FrameType.REQUEST, i, "test request")));
        }

        for (CompletableFuture<Frame> f : buffer) {
            try {
                Frame response = f.get(3, TimeUnit.SECONDS);
                System.out.println("Received frame: " + response);
                System.out.println("Received frame payload: " + response.getPayload());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace(System.out);
            }
        }
    }
}

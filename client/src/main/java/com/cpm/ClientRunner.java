package com.cpm;

import com.google.common.base.Stopwatch;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientRunner {

    private static final Logger logger = LoggerFactory.getLogger(ClientRunner.class);

    public static void main(String[] args) throws Exception {
        String host;
        int port;

        if (args.length != 2) {
            host = "localhost";
            port = 8080;
        } else {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);

        ClientBuilder builder = new ClientBuilder();
        Client client = null;

        Stopwatch watch = Stopwatch.createStarted();
        try {
            client = builder.connect(new InetSocketAddress(host, port),
                    5000 /*Connection timeout in millis*/,
                    1000   /*max pending limit*/,
                    20   /*heartbeat seconds*/);
            watch.stop();
            logger.debug("Connected. Time to connect: " + watch.elapsed(TimeUnit.SECONDS) + " second(s)");

            //runSend(client, 110);
            //runContinuousSend(client);
        } finally {
            if (watch.isRunning()) {
                watch.stop();
                logger.debug("Time wait to connect: " + watch.elapsed(TimeUnit.SECONDS) + " second(s)");
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

        try {
            for (int i = 1; i <= numberOfTimes; i++) {
                logger.debug("Send frame #: " + i);
                buffer.add(client.send(new Frame(FrameType.REQUEST, i, "test request")));
            }
        } catch (ClientException cex) {
            logger.error("Error sending: ", cex);
        }

        for (CompletableFuture<Frame> f : buffer) {
            try {
                Frame response = f.get(3, TimeUnit.SECONDS);
                logger.debug("Received: " + response + ", Payload: " + response.getPayload());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Error getting response: ", e);
            }
        }
    }

    private static void runContinuousSend(Client client) throws InterruptedException {
        try {
            int i = 1;
            while (true) {
                client.send(new Frame(FrameType.REQUEST, i, "test request"))
                        .thenAccept(f -> logger.debug("Received: " + f + ", Payload: " + f.getPayload()));
                i++;
            }
        } catch (ClientException cex) {
            logger.error("Error: ", cex);
        }
    }
}

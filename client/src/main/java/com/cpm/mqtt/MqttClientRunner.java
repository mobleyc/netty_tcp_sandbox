package com.cpm.mqtt;


import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class MqttClientRunner {

    private static final Logger logger = LoggerFactory.getLogger(MqttClientRunner.class);

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

        Stopwatch watch = Stopwatch.createStarted();

        EventLoopGroup group = new NioEventLoopGroup(0,
                new ThreadFactoryBuilder().setNameFormat("nio-worker" + "-%d").build());

        List<MqttTopicSubscription> topicSubscriptions = new LinkedList<>();
        topicSubscriptions.add(new MqttTopicSubscription("test/#", MqttQoS.AT_MOST_ONCE));

        MqttClient client = null;
        try {
            InetSocketAddress address = new InetSocketAddress(host, port);
            int connectTimeoutMilliseconds = 5000;

            MqttClientBuilder builder = new MqttClientBuilder(group);
            CompletableFuture<MqttClient> clientF = builder.connect(address, connectTimeoutMilliseconds);
            //TODO: Add to config - MQTT connect timeout
            client = clientF.get(3, TimeUnit.SECONDS);
            watch.stop();
            logger.debug("Connected. Time to connect: " + watch.elapsed(TimeUnit.SECONDS) + " second(s)");

            CompletableFuture<Void> sub = client.subscribe(topicSubscriptions);
            //TODO: Add to config - Subscribe timeout
            sub.get(3, TimeUnit.SECONDS);

            client.publish(createPublishMessage("test", "test from netty", false));
            logger.debug("Published message");

            client.sync();

        } catch (Exception ex) {
            System.err.println("Error: ");
            ex.printStackTrace();
        } finally {
            if (watch.isRunning()) {
                watch.stop();
                logger.debug("Time wait to connect: " + watch.elapsed(TimeUnit.SECONDS) + " second(s)");
            }

            if(client != null) {
                logger.debug("Disconnecting");
                client.disconnect();
            }

            group.shutdownGracefully();
        }
    }

    private static final ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);

    private static MqttPublishMessage createPublishMessage(String topicName, String payload, boolean isRetained) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBLISH, false /*isDup*/, MqttQoS.AT_MOST_ONCE,
                        isRetained, 0 /*remainingLength*/);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, 456);
        ByteBuf payloadBuffer =  ALLOCATOR.buffer();
        payloadBuffer.writeBytes(payload.getBytes(CharsetUtil.UTF_8));
        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, payloadBuffer);
    }
}

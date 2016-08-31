package com.cpm.mqtt;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public class MqttClientBuilder {

    //TODO: Move to configuration
    private static final String CLIENT_ID = "TEST_CLIENT";
    private static final String WILL_TOPIC = "TEST/LWT";
    private static final String WILL_MESSAGE = "TEST_CLIENT gone";
    private static final String USER_NAME = null;
    private static final String PASSWORD = null;

    //TODO: Move to configuration
    private static final int KEEP_ALIVE_SECONDS = 120;

    private static final String MQTT_CONNECT_HANDLER = "MQTT_CONNECT";

    private static final Logger logger = LoggerFactory.getLogger(MqttClientBuilder.class);

    private final EventLoopGroup group;

    public MqttClientBuilder(EventLoopGroup group) {
        this.group = group;
    }

    /**
     * Connect asynchronously to an MQTT server.
     *
     * @param address - Broker address to connect to
     * @param connectTimeoutMilliseconds - IO connection timeout
     * @return - A future for MqttClient
     */
    public CompletableFuture<MqttClient> connect(InetSocketAddress address, int connectTimeoutMilliseconds) {
        Bootstrap b = createBootstrap(group, address, connectTimeoutMilliseconds);

        CompletableFuture<MqttClient> mqttConnection = new CompletableFuture<>();

        ChannelFuture tcpConnect = b.connect();
        tcpConnect.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                Channel ch = future.channel();
                ChannelPipeline p = ch.pipeline();
                p.addLast(MQTT_CONNECT_HANDLER, new ClientConnectionSetupHandler(address, mqttConnection));
            } else {
                logger.debug("TCP Connection failed.");
                mqttConnection.completeExceptionally(new RuntimeException("Unable to connect to server."));
            }
        });

        return mqttConnection;
    }

    private Bootstrap createBootstrap(EventLoopGroup group, InetSocketAddress address, int connectTimeoutMilliseconds) {
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMilliseconds)
                .remoteAddress(address)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(MqttEncoder.INSTANCE);
                        p.addLast(new MqttDecoder());
                    }
                });
        return b;
    }

    /**
     * Handler used to setup an MQTT connection once a TCP connection is established. The handler
     * will remove itself from the channel pipeline once an MQTT CONNACK is received by the server.
     */
    private class ClientConnectionSetupHandler extends SimpleChannelInboundHandler<MqttMessage> {

        private final InetSocketAddress address;
        private final CompletableFuture<MqttClient> connection;

        public ClientConnectionSetupHandler(InetSocketAddress address, CompletableFuture<MqttClient> connection) {
            this.address = address;
            this.connection = connection;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.debug("Channel active.");

            MqttConnectMessage connectMsg = createConnectMessage(MqttVersion.MQTT_3_1_1);
            ctx.writeAndFlush(connectMsg).addListener((ChannelFutureListener) conFuture -> {
                if (!conFuture.isSuccess()) {
                    logger.debug("MQTT CONNECT message send to OS failed.");
                }
            });
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MqttMessage mqttMessage) throws Exception {
            logger.debug("Channel read. Message Type: " + mqttMessage.fixedHeader().messageType());

            if(mqttMessage.fixedHeader().messageType() == MqttMessageType.CONNACK) {
                logger.debug("Received MQTT CONNACK");

                connection.complete(new MqttClient(ctx.channel(), address, 1000/*pendingRequestLimit*/, KEEP_ALIVE_SECONDS));
                ctx.pipeline().remove(MQTT_CONNECT_HANDLER);
                logger.debug("Handler removed from channel: " + MQTT_CONNECT_HANDLER);
            }
        }
    }

    private static MqttConnectMessage createConnectMessage(MqttVersion mqttVersion) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
                new MqttConnectVariableHeader(
                        mqttVersion.protocolName(),
                        mqttVersion.protocolLevel(),
                        false /*hasUserName*/,
                        false /*hasPassword*/,
                        false /*isWillRetain*/,
                        0 /*willQos*/,
                        true /*isWillFlag*/,
                        true /*isCleanSession*/,
                        KEEP_ALIVE_SECONDS);
        MqttConnectPayload payload =
                new MqttConnectPayload(CLIENT_ID, WILL_TOPIC, WILL_MESSAGE, USER_NAME, PASSWORD);
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, payload);
    }
}

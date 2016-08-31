package com.cpm.mqtt;


import com.cpm.ClientException;
import com.cpm.ConnectionException;
import io.netty.channel.*;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.handler.timeout.IdleState.WRITER_IDLE;

/**
 * This class is invoked by the main client thread
 */
public class MqttClient {

    private static final ClientException MAX_PENDING_EXCEPTION =
            new ClientException("Max pending request limit reached.");

    private static final Logger logger = LoggerFactory.getLogger(MqttClient.class);

    private final Channel channel;

    private int messageId = 0;
    private final ConcurrentHashMap<Integer, CompletableFuture<Void>> pendingRequests =
            new ConcurrentHashMap<>();
    private final InetSocketAddress address;
    private final int pendingRequestLimit;

    //ConcurrentHashMap.size() is transient and not synchronized.
    //    ref: ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html
    private final AtomicInteger pendingRequestCounter = new AtomicInteger();

    private volatile ScheduledFuture<?> pingTimeout;

    private final AtomicBoolean disconnectPacketSent = new AtomicBoolean(false);

    /**
     * Setup MQTT client.
     *
     * Invariants:
     *     1. An MQTT connection has been established before initializing this class.
     *
     * @param channel - An exising TCP connection that has already negotiated MQTT Connect and Connack with the MQTT
     *                server.
     */
    MqttClient(Channel channel, InetSocketAddress address, int pendingRequestLimit, int heartBeatIntervalSeconds) {
        this.channel = channel;
        this.address = address;
        this.pendingRequestLimit = pendingRequestLimit;

        ChannelPipeline p = channel.pipeline();
        if(0 != heartBeatIntervalSeconds) {
            /*
             * Note: The spec suggests that Keep Alives are only triggered when a client has not sent a control
             * packet to the server within some amount of time. As a result, we're only using idle write timeouts
             * to trigger an MQTT PINGREQUEST.
             *
             * Spec: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/csprd02/mqtt-v3.1.1-csprd02.html#_Toc385349766 - See
             * "3.1.2.10 Keep Alive"
             */
            p.addLast("idleStateHandler", new IdleStateHandler(0, heartBeatIntervalSeconds, 0));
        }
        p.addLast(new ClientConnectionHandler(heartBeatIntervalSeconds));
    }

    public CompletableFuture<Void> subscribe(List<MqttTopicSubscription> subscriptions) {

        if (!channel.isWritable()) {
            throw new ConnectionException(address, "Attempted to write to a closed connection");
        }

        if (!tryIncrementPending()) {
            logger.debug("Max limit reached");
            close();

            throw MAX_PENDING_EXCEPTION;
        }

        CompletableFuture<Void> result = new CompletableFuture<>();

        //TODO: Move to generic send method?
        this.messageId = getNextMessageId(this.messageId);
        MqttMessageIdVariableHeader id = MqttMessageIdVariableHeader.from(this.messageId);
        MqttSubscribeMessage subRequest = createSubscribeMessage(subscriptions, id);

        this.channel.writeAndFlush(subRequest).addListener((ChannelFutureListener) subF -> {
            if (subF.isSuccess()) {
                CompletableFuture<Void> old = pendingRequests.put(id.messageId(), result);
                if(null != old) {
                    logger.debug("Unexpected state on pending.put");
                    result.completeExceptionally(new ClientException("Failed to send subscribe message to OS"));
                    close();
                }

                logger.debug("MQTT: SUBSCRIBE message sent to OS.");
            } else {
                result.completeExceptionally(new ClientException("Failed to send subscribe message to OS"));
                close();
            }
        });

        return result;
    }

    /**
     * Publish a message that uses MQTT QOS 0.
     *
     * @param message - Message to publish
     */
    public void publish(MqttPublishMessage message) {
        this.channel.writeAndFlush(message).addListener((ChannelFutureListener) subF -> {
            if (!subF.isSuccess()) {
                logger.error("MQTT: " + message.fixedHeader().messageType() + " message failed to send to OS.");
                close();
            }
        });
    }

    public void sync() throws InterruptedException {
        this.channel.closeFuture().sync();
    }

    //Disconnect from MQTT broker and close TCP connection
    public void disconnect() {

        boolean isPrevDisconnected = disconnectPacketSent.getAndSet(true);
        if(isPrevDisconnected) {
            throw new ClientException("Client is already disconnected.");
        }

        cancelPingTimer();

        MqttMessage disconnect = createMessageWithFixedHeader(MqttMessageType.DISCONNECT);
        this.channel.writeAndFlush(disconnect).addListener((ChannelFutureListener) subF -> {
            if (!subF.isSuccess()) {
                logger.error("MQTT: " + disconnect.fixedHeader().messageType() + " message failed to send to OS.");
                close();
            } else {
                //TODO: Add grace period before closing?
                channel.close().syncUninterruptibly();
            }
        });
    }

    //Close the TCP connection
    private void close() {
        channel.close().syncUninterruptibly();
    }

    private boolean tryIncrementPending() {
        int pending;
        do {
            pending = pendingRequestCounter.get();
            if (pending >= pendingRequestLimit) {
                return false;
            }
        } while (!pendingRequestCounter.compareAndSet(pending, pending + 1));
        return true;
    }

    private void cancelPingTimer() {
        if (pingTimeout != null && !pingTimeout.isDone()) {
            pingTimeout.cancel(true);
            pingTimeout = null;
        }
    }

    /**
     * This class is invoked by the Netty IO thread
     */
    private class ClientConnectionHandler extends SimpleChannelInboundHandler<MqttMessage> {

        private final int heartBeatIntervalSeconds;

        ClientConnectionHandler(int heartBeatIntervalSeconds) {
            this.heartBeatIntervalSeconds = heartBeatIntervalSeconds;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MqttMessage mqttMessage) throws Exception {
            logger.debug("Channel read. Message Type: " + mqttMessage.fixedHeader().messageType());

            if (mqttMessage.fixedHeader().messageType() == MqttMessageType.SUBACK) {
                MqttMessageIdVariableHeader id = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
                logger.debug("Received SUBACK for message id: " + id.messageId());

                CompletableFuture<Void> inboundF = pendingRequests.remove(id.messageId());
                if (inboundF == null) {
                    close();
                    //TODO: It might be possible for a SUBACK to come in from a broken connection that previously sent
                    // a subscribe?
                    throw new ClientException(new IllegalStateException("Received message for unknown message id. " +
                            "Message Id: " + id.messageId()));
                }

                pendingRequestCounter.decrementAndGet();
                inboundF.complete(null);
            } else if (mqttMessage.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
                MqttPublishMessage publish = (MqttPublishMessage) mqttMessage;

                if(logger.isDebugEnabled()) {
                    StringBuilder buffer = new StringBuilder();
                    buffer.append("Msg - Topic Name=").append(publish.variableHeader().topicName())
                            .append(", QOS Level=").append(publish.fixedHeader().qosLevel())
                            .append(", Is Duplicate=").append(publish.fixedHeader().isDup())
                            .append(", Is Retain=").append(publish.fixedHeader().isRetain());
                    if (publish.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
                        buffer.append("Message Id: ").append(publish.variableHeader().messageId());
                    }
                    logger.debug(buffer.toString());
                }

                byte[] bytes = new byte[publish.payload().readableBytes()];
                publish.payload().readBytes(bytes);
                logger.debug("Payload: " + new String(bytes));
            } else if(mqttMessage.fixedHeader().messageType() == MqttMessageType.PINGRESP) {
                cancelPingTimer();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            /*
             * If a TCP connection is dropped without a FIN being sent, then an IOException will be caught here
             * once the OS recognizes the failure. You can test this by creating a connection to a server running
             * in a VM and then shutting down the server via the VM interface (e.g. VMWare console). See also:
             *     http://dtrace.org/blogs/dap/2016/08/18/tcp-puzzlers/
             *
             * Example of the exception raised:
             *   java.io.IOException: An existing connection was forcibly closed by the remote host
             */
            logger.error("Error: ", cause);
            close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            if(!(evt instanceof IdleStateEvent)) {
                return;
            }

            IdleStateEvent isEvt = (IdleStateEvent)evt;
            logger.debug("Received idle state event: " + isEvt.state());

            if(isEvt.state() != WRITER_IDLE) {
                return;
            }

            boolean isWritable = ctx.channel().isWritable();
            if (!isWritable) {
                logger.debug("Received idle state event, but connection is closed. Heartbeat message will not be sent.");
                return;
            }

            if(disconnectPacketSent.get()) {
                logger.debug("Received idle state event, but connection is disconnected. Heartbeat message will not be sent.");
                return;
            }

            MqttMessage pingRequest = createMessageWithFixedHeader(MqttMessageType.PINGREQ);
            ctx.channel().writeAndFlush(pingRequest).addListener((ChannelFutureListener) subF -> {
                if (subF.isSuccess()) {
                    //The MQTT spec does not define how long to wait for PINGRESP
                    //TODO: Make config setting for PING timeout
                    EventExecutor loop = ctx.executor();
                    pingTimeout = loop.schedule(new PingRespTimeoutTask(), this.heartBeatIntervalSeconds, TimeUnit.SECONDS);

                    logger.debug("MQTT: PINGREQ message sent to OS. PINGRESP timer created.");
                } else {
                    logger.debug("MQTT: PINGREQ message failed to send to OS.");
                    close();
                }
            });
        }
    }

    private static MqttSubscribeMessage createSubscribeMessage(List<MqttTopicSubscription> subscriptions,
                                                               MqttMessageIdVariableHeader messageId) {
        /*
         * Netty API notes compared to Spec:
         *   You can use the raw MqttFixedHeader message however it needs to be set according to the spec, which
         *   the API doesn't match well.
         *
         * Per 3.1.1 spec: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/csprd02/mqtt-v3.1.1-csprd02.html#_Toc385349799
         *   Bits 3,2,1 and 0 of the fixed header of the SUBSCRIBE Control Packet are reserved and MUST be set to
         *   0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network
         *   Connection.
         *
         * In this case:
         *   Bit 3 = 0 => Is Duplicate is set to false
         *   Bits 2 and 1 = 01 => QOS is set to AT_LEAST_ONCE
         *   Bit 0 = 0 => Is Retain is set to false
         */
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false /*is duplicate*/, MqttQoS.AT_LEAST_ONCE,
                        false /*is retain*/, 0 /*remainingLength*/);
        MqttSubscribePayload mqttSubscribePayload = new MqttSubscribePayload(subscriptions);
        return new MqttSubscribeMessage(mqttFixedHeader, messageId, mqttSubscribePayload);
    }

    private static MqttMessage createMessageWithFixedHeader(MqttMessageType messageType) {
        return new MqttMessage(new MqttFixedHeader(messageType, false /*isDup*/, MqttQoS.AT_MOST_ONCE,
                false /*isRetain*/, 0 /*remainingLength*/));
    }

    // Generate and MQTT 16-bit Packet Identifier
    private int getNextMessageId(int messageId) {
        if(messageId <= 0 || messageId > '\uffff') {
            messageId = 1;
        } else {
            messageId = messageId + 1;
        }

        return messageId;
    }

    /**
     * This class is used to time out a PINGREQ. An instance is scheduled once a PINGREQ is sent. If a PINGRESP
     * is received before the instance is executd, then the instance is cancelled. Otherwise it is assumed a
     * PINGRESP was never received and when the instance executes the connection will close.
     */
    private class PingRespTimeoutTask implements Runnable {

        public void run() {
            logger.error("MQTT PINGREQ timed out. Closing connection.");
            close();
        }
    }
}

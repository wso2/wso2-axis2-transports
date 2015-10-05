package org.apache.axis2.transport.rabbitmq.rpc;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

public class RPCChannel {

    private Channel channel;
    private QueueingConsumer consumer;
    private String replyToQueue;
    private Connection connection;

    public RPCChannel(Connection connection, Channel channel, QueueingConsumer consumer, String replyToQueue) {
        this.channel = channel;
        this.consumer = consumer;
        this.replyToQueue = replyToQueue;
        this.connection = connection;
        try {
            this.channel.basicConsume("REPLYQ", false, consumer);//TODO remove this from constructor. Start consuming at send?
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isOpen() {
        if (!channel.isOpen()) {
            try {
                connection.createChannel();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public Channel getChannel() {
        if (!channel.isOpen()) {
            try {
                connection.createChannel();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
        return channel;
    }

    public QueueingConsumer getConsumer() {
        return consumer;
    }

    public String getReplyToQueue() {
        return replyToQueue;
    }
}

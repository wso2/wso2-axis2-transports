package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

public class RMQChannel {

    private com.rabbitmq.client.Channel channel;
    private Connection connection;

    public RMQChannel(Connection connection, com.rabbitmq.client.Channel channel, QueueingConsumer consumer, String replyToQueue) {
        this.channel = channel;
        this.connection = connection;
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

    public com.rabbitmq.client.Channel getChannel() {
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

}

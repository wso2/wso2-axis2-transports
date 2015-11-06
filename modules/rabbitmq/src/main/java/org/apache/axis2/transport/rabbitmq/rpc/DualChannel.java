package org.apache.axis2.transport.rabbitmq.rpc;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.axis2.transport.rabbitmq.RMQChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class DualChannel extends RMQChannel {

    private static final Log log = LogFactory.getLog(DualChannel.class);

    private String replyToQueue;
    private QueueingConsumer consumer;

    public DualChannel(Connection connection, Channel channel, QueueingConsumer consumer, String replyToQueue) {
        super(connection, channel);

        this.replyToQueue = replyToQueue;
        this.consumer = consumer;

        try {
            getChannel().basicConsume(replyToQueue, true, consumer);
        } catch (IOException e) {
            log.error("Error initiating consumer for queue " + replyToQueue, e);
        }
    }

    public QueueingConsumer getConsumer() {
        return consumer;
    }

    public String getReplyToQueue() {
        return replyToQueue;
    }
}

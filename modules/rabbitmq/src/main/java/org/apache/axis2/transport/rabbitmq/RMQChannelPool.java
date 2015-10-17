package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class RMQChannelPool {

    private BlockingDeque<RMQChannel> RMQChannelPool;

    public RMQChannelPool(RabbitMQConnectionFactory connectionFactory, int connectionPoolSize) {

        RMQChannelPool = new LinkedBlockingDeque<>();
        try {
            Connection connection = connectionFactory.createConnection();
            for (int i = 0; i < connectionPoolSize; i++) {
                com.rabbitmq.client.Channel channel = connection.createChannel();
                QueueingConsumer consumer = new QueueingConsumer(channel);
                String replyQueueName = channel.queueDeclare().getQueue();
                channel.basicConsume(replyQueueName, false, consumer);
                RMQChannelPool.add(new RMQChannel(connection, channel));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public RMQChannel take() throws InterruptedException {
        return RMQChannelPool.take();
    }

    public void push(RMQChannel RMQChannel) {
        RMQChannelPool.push(RMQChannel);
    }

    public void clear() {
        RMQChannelPool.clear();
    }
}

package org.apache.axis2.transport.rabbitmq.rpc;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.axis2.transport.rabbitmq.RabbitMQConnectionFactory;
import org.apache.axis2.transport.rabbitmq.utils.AxisRabbitMQException;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class DualChannelPool {

    private BlockingDeque<DualChannel> dualChannelPool;

    public DualChannelPool(RabbitMQConnectionFactory connectionFactory, int connectionPoolSize) {

        dualChannelPool = new LinkedBlockingDeque<>();
        try {
            Connection connection = connectionFactory.createConnection();
            for (int i = 0; i < connectionPoolSize; i++) {
                Channel channel = connection.createChannel();
                QueueingConsumer consumer = new QueueingConsumer(channel);
                String replyQueueName = channel.queueDeclare().getQueue();
                dualChannelPool.add(new DualChannel(connection, channel, consumer, replyQueueName));
            }
        } catch (IOException e) {
            throw new AxisRabbitMQException("Error creating dual channel pool", e);
        }
    }

    public DualChannel take() throws InterruptedException {
        return dualChannelPool.take();
    }

    public void push(DualChannel dualChannel) {
        dualChannelPool.push(dualChannel);
    }

    public void clear() {
        dualChannelPool.clear();
    }
}

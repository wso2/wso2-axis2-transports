package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class RMQChannelPool {

    private static final Log log = LogFactory.getLog(RMQChannelPool.class);
    private BlockingDeque<RMQChannel> RMQChannelPool;

    public RMQChannelPool(RabbitMQConnectionFactory connectionFactory, int connectionPoolSize) {

        RMQChannelPool = new LinkedBlockingDeque<>();
        try {
            Connection connection = connectionFactory.createConnection();
            for (int i = 0; i < connectionPoolSize; i++) {
                com.rabbitmq.client.Channel channel = connection.createChannel();
                RMQChannelPool.add(new RMQChannel(connection, channel));
            }
        } catch (IOException e) {
            log.error("Exception occurred while creating connection", e);
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

package org.apache.axis2.transport.rabbitmq.rpc;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.axis2.transport.rabbitmq.RabbitMQConnectionFactory;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class DualChannelPool {

    private BlockingDeque<DualChannel> dualChannelPool;

    public DualChannelPool(RabbitMQConnectionFactory connectionFactory) {

        dualChannelPool = new LinkedBlockingDeque<>();
        //TODO : connection recovery
        try {
            Connection connection = connectionFactory.createConnection();
            for (int i = 0; i < 200; i++) {
                Channel channel = connection.createChannel();
                QueueingConsumer consumer = new QueueingConsumer(channel);
                String replyQueueName = channel.queueDeclare().getQueue();
                channel.basicConsume(replyQueueName, false, consumer);
                dualChannelPool.add(new DualChannel(connection, channel, consumer, replyQueueName));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public DualChannel take() throws InterruptedException {
        return dualChannelPool.take();
    }

    public void push(DualChannel dualChannel){
         dualChannelPool.push(dualChannel);
    }

    public void clear(){
        dualChannelPool.clear();
    }
}

package org.apache.axis2.transport.rabbitmq.rpc;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.axis2.transport.rabbitmq.RabbitMQConnectionFactory;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class RPCChannelPool {

    private BlockingDeque<RPCChannel> rpcChannelPool;

    public RPCChannelPool(RabbitMQConnectionFactory connectionFactory) {

        rpcChannelPool = new LinkedBlockingDeque<>();
        //TODO : connection recovery
        try {
            Connection connection = connectionFactory.createConnection();
            for (int i = 0; i < 200; i++) {
                Channel channel = connection.createChannel();
                QueueingConsumer consumer = new QueueingConsumer(channel);
                String replyQueueName = channel.queueDeclare().getQueue();
                channel.basicConsume(replyQueueName, false, consumer);
                rpcChannelPool.add(new RPCChannel(connection, channel, consumer, replyQueueName));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public RPCChannel take() throws InterruptedException {
        return rpcChannelPool.take();
    }

    public void push(RPCChannel rpcChannel){
         rpcChannelPool.push(rpcChannel);
    }

    public void clear(){
        rpcChannelPool.clear();
    }
}

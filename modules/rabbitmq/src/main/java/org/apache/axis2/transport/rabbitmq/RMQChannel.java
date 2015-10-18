package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class RMQChannel {

    private static final Log log = LogFactory.getLog(RMQChannel.class);

    private Channel channel;
    private Connection connection;

    public RMQChannel(Connection connection, Channel channel) {
        this.channel = channel;
        this.connection = connection;
    }

    /**
     * If channel is closed, recreate the channel and return availability
     *
     * @return true if channel is open and false if channel is closed.
     */
    public boolean isOpen() {
        if (!channel.isOpen()) {
            try {
                connection.createChannel();
            } catch (IOException e) {
                log.error("Error creating channel for dual channel", e);
                return false;
            }
        }
        return true;
    }

    /**
     * If channel is closed, recreate the channel and return.
     *
     * @return an open channel
     */
    public Channel getChannel() {
        if (!channel.isOpen()) {
            try {
                log.debug("Channel is closed. Creating a new channel");
                connection.createChannel();
            } catch (IOException e) {
                log.error("Error creating channel for dual channel", e);
                return null;
            }
        }
        return channel;
    }

}

package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class RMQChannel {

    private static final Log log = LogFactory.getLog(RMQChannel.class);

    private Channel channel;
    private Connection connection;
    private int qos = -1; //-1 means qos is not specified, in that case only basic qos will be applied to channel

    public RMQChannel(Connection connection, Channel channel) {
        this.channel = channel;
        this.connection = connection;
    }

    /**
     * Constructor with just connection and qos parameters
     * This constructor will create the channel and if applicable
     * apply qos as well
     *
     * @param connection
     * @param qos
     * @throws IOException
     */
    public RMQChannel(Connection connection, int qos) throws IOException{
        this.qos = qos;
        this.connection = connection;
        this.channel = this.connection.createChannel();
        if (this.qos > 0) {
            this.channel.basicQos(this.qos);
        }
    }

    /**
     * If channel is closed, recreate the channel and return availability
     *
     * @return true if channel is open and false if channel is closed.
     */
    public boolean isOpen() {
        if (!channel.isOpen()) {
            try {
                channel = connection.createChannel();
                if (this.qos > 0) {
                    channel.basicQos(this.qos);
                }
            } catch (IOException e) {
                log.error("Error creating channel for RMQ channel", e);
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
                if (log.isDebugEnabled()) {
                    log.debug("Channel is closed. Creating a new channel");
                }
                channel = connection.createChannel();
                //If qos is applicable, then apply qos before returning the channel
                if (this.qos > 0) {
                    channel.basicQos(this.qos);
                }
            } catch (IOException e) {
                log.error("Error creating channel for RMQ channel", e);
                return null;
            }
        }
        return channel;
    }

    /**
     * Helper method to close connection in RMQChannel
     *
     * @throws ShutdownSignalException
     * @throws IOException
     * @throws AlreadyClosedException
     */
    public void closeConnection() throws ShutdownSignalException, IOException, AlreadyClosedException{
        if (connection != null) {
            try {
                channel.close();
            } catch (Exception e) {
                //ignore as the connection gets closed anyway
            }
            channel = null;
            try {
                connection.close();
            } finally {
                connection = null;
            }
        }
    }
}

/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Class that performs the actual sending of a RabbitMQ AMQP message,
 */
public class RabbitMQMessageSender {
    private static final Log log = LogFactory.getLog(RabbitMQMessageSender.class);

    private Channel channel;
    private String factoryName;
    private boolean channelChanged = false;
    private RabbitMQSender.SenderType senderType;
    private final RabbitMQChannelPool rabbitMQChannelPool;
    private final RabbitMQChannelPool rabbitMQConfirmChannelPool;
    private TimeoutRegistry timeoutRegistry = null;
    private InflightThreads inflightThreads = null;

    /**
     * Create a RabbitMQSender using a ConnectionFactory and target EPR.
     *
     * @param channel                    the {@link Channel} object
     * @param factoryName                the connection factory name
     * @param senderType                 the type of the sender to execute the different logic
     * @param rabbitMQChannelPool        rabbitmq channel  pool
     * @param rabbitMQConfirmChannelPool rabbitmq confirm channel  pool
     */
    public RabbitMQMessageSender(Channel channel, String factoryName, RabbitMQSender.SenderType senderType,
                                 RabbitMQChannelPool rabbitMQChannelPool,
                                 RabbitMQChannelPool rabbitMQConfirmChannelPool) {
        this.channel = channel;
        this.senderType = senderType;
        this.factoryName = factoryName;
        this.rabbitMQChannelPool = rabbitMQChannelPool;
        this.rabbitMQConfirmChannelPool = rabbitMQConfirmChannelPool;
    }

    /**
     * Create a RabbitMQSender using a ConnectionFactory and target EPR.
     *
     * @param channel                    the {@link Channel} object
     * @param factoryName                the connection factory name
     * @param senderType                 the type of the sender to execute the different logic
     * @param rabbitMQChannelPool        rabbitmq channel  pool
     * @param rabbitMQConfirmChannelPool rabbitmq confirm channel  pool
     * @param timeoutRegistry            timeout registry to keep track of timeouts
     * @param inflightThreads           inflight channels registry to keep track of rpc channels
     */
    public RabbitMQMessageSender(Channel channel, String factoryName, RabbitMQSender.SenderType senderType,
                                 RabbitMQChannelPool rabbitMQChannelPool,
                                 RabbitMQChannelPool rabbitMQConfirmChannelPool, TimeoutRegistry timeoutRegistry,
                                 InflightThreads inflightThreads) {
        this.channel = channel;
        this.senderType = senderType;
        this.factoryName = factoryName;
        this.rabbitMQChannelPool = rabbitMQChannelPool;
        this.rabbitMQConfirmChannelPool = rabbitMQConfirmChannelPool;
        this.timeoutRegistry = timeoutRegistry;
        this.inflightThreads = inflightThreads;
    }

    /**
     * Publish message to the exchange with the routing key. Execute relevant logic based on the sender type.
     *
     * @param routingKey         the routing key to publish the message
     * @param msgContext         the axis2 message context
     * @param rabbitMQProperties the rabbitmq endpoint parameters
     * @return {@link Delivery} in RPC messaging style
     * @throws AxisRabbitMQException
     * @throws IOException
     * @throws InterruptedException
     */
    // TODO: handle x-consistent-hash
    public Delivery send(String routingKey, MessageContext msgContext,
                         Map<String, String> rabbitMQProperties) throws Exception {

        Delivery response = null;
        // declaring queue if given
        String queueName = rabbitMQProperties.get(RabbitMQConstants.QUEUE_NAME);
        String exchangeName = rabbitMQProperties.get(RabbitMQConstants.EXCHANGE_NAME);
        String messageId = msgContext.getMessageID();

        try {
            if (!RabbitMQAckConfig.isAvoidDeclaringExchangesQueuesWhenPublishing()) {
                try {
                    RabbitMQUtils.declareQueue(channel, queueName, rabbitMQProperties);
                } catch (IOException ex) {
                    channel = checkAndIgnoreInEquivalentParamException(ex, RabbitMQConstants.QUEUE, queueName);
                }
                try {
                    RabbitMQUtils.declareExchange(channel, exchangeName, rabbitMQProperties);
                } catch (IOException ex) {
                    channel = checkAndIgnoreInEquivalentParamException(ex, RabbitMQConstants.EXCHANGE, exchangeName);
                }
            }
            RabbitMQUtils.bindQueueToExchange(channel, queueName, exchangeName, rabbitMQProperties);

            AMQP.BasicProperties.Builder builder = buildBasicProperties(msgContext);

            String messageType = rabbitMQProperties.get(RabbitMQConstants.MESSAGE_TYPE);
        if (messageType != null) {
            builder.type(messageType);
        }

        int deliveryMode = NumberUtils.toInt(rabbitMQProperties.get(RabbitMQConstants.QUEUE_DELIVERY_MODE),
                RabbitMQConstants.DEFAULT_DELIVERY_MODE);
        builder.deliveryMode(deliveryMode);

            String replyTo = rabbitMQProperties.get(RabbitMQConstants.REPLY_TO_QUEUE_NAME);
            if (StringUtils.isEmpty(replyTo)) {
                builder.replyTo(replyTo);
            }

            AMQP.BasicProperties basicProperties = builder.build();
            byte[] messageBody = RabbitMQUtils.getMessageBody(msgContext);

            switch (senderType) {
                case RPC:
                    long replyTimeout = NumberUtils
                            .toLong((String) msgContext.getProperty(RabbitMQConstants.RABBITMQ_WAIT_REPLY));
                    if (replyTimeout <= 0) {
                        replyTimeout = NumberUtils
                                .toLong(rabbitMQProperties.get(RabbitMQConstants.RABBITMQ_WAIT_REPLY_TIMEOUT),
                                        RabbitMQConstants.DEFAULT_RABBITMQ_TIMEOUT);
                    }
                    response = sendRPC(exchangeName, routingKey, basicProperties, messageBody, replyTimeout);
                    break;
                case PUBLISHER_CONFIRMS:
                    long confirmTimeout = NumberUtils
                            .toLong((String) msgContext.getProperty(RabbitMQConstants.RABBITMQ_WAIT_CONFIRMS));
                    if (confirmTimeout <= 0) {
                        confirmTimeout = NumberUtils.toLong(rabbitMQProperties
                                        .get(RabbitMQConstants.RABBITMQ_PUBLISHER_CONFIRMS_WAIT_TIMEOUT),
                                RabbitMQConstants.DEFAULT_RABBITMQ_TIMEOUT);
                    }
                    if (RabbitMQAckConfig.isCallbackControlledAckEnabled()) {
                        confirmTimeout = RabbitMQAckConfig.getDefaultPublisherConfirmsTimeoutWhenCallbackEnabledMs();
                        // Register BEFORE publish so onAppError can always find/close it
                        inflightThreads.register(messageId, Thread.currentThread());

                        // Re-check AFTER register to catch timeouts that landed just now
                        if (timeoutRegistry.isTimedOut(messageId) || Thread.currentThread().isInterrupted()) {
                            log.warn("Message with ID " + messageId
                                    + " already Timed Out BEFORE PUBLISH, hence dropping it");
                            // Already timed out, so throw exception to skip publish & drop it
                            throw new AxisRabbitMQException("Message with ID " + messageId +
                                    " already Timed Out BEFORE PUBLISH, hence dropping itt");
                        }

                    }
                    sendPublisherConfirms(exchangeName, routingKey, basicProperties, messageBody, confirmTimeout);
                    break;
                default:
                    publishMessage(exchangeName, routingKey, basicProperties, messageBody);
                    break;
            }

            return response;

        } catch (Exception e) {
            if (channelChanged && channel != null) {
                log.warn("Invalidating intermediate channel: " + channel + " due to exception: " + e.getMessage());
                invalidateChannel(senderType, factoryName, channel);
                channel = null;
            }
            throw e;
        } finally {
            if (channelChanged && channel != null) {
                returnToPool(factoryName, channel, senderType);
            }
        }
    }

    /**
     * The channel will return to pool or destroy
     *
     * @param factoryName pool key
     * @param channel     instance to return to the keyed pool
     * @param senderType  the type of the sender to select the relevant pool
     */
    private void returnToPool(String factoryName, Channel channel, RabbitMQSender.SenderType senderType) {
        if (senderType == RabbitMQSender.SenderType.PUBLISHER_CONFIRMS) {
            rabbitMQConfirmChannelPool.returnObject(factoryName, channel);
        } else {
            rabbitMQChannelPool.returnObject(factoryName, channel);
        }
    }

    private Channel checkAndIgnoreInEquivalentParamException(IOException ex, String entity,
                                                             String queueOrExchangeName) throws Exception {

        String cause = ex.getCause() != null ? ex.getCause().getMessage() : null;
        if (cause != null && cause.contains(RabbitMQConstants.IN_EQUIVALENT_ARGUMENT_ERROR)) {
            // if already assigned a new channel then we need to invalidate that as well.
            //So new one will be either returned or destroyed ath the finally block of the send method
            //The oldest reference will be destroyed or returned to the pool by RabbitMq Sender
            if (channelChanged && channel != null) {
                invalidateChannel(senderType, factoryName, channel);
                channel = null;
            }
            // borrowing a new channel as the existing one is closed due to exception
            Channel newChannel;
            if (senderType == RabbitMQSender.SenderType.PUBLISHER_CONFIRMS) {
                newChannel = rabbitMQConfirmChannelPool.borrowObject(factoryName);
            } else {
                newChannel = rabbitMQChannelPool.borrowObject(factoryName);
            }
            channelChanged = true;
            if (log.isDebugEnabled()) {
                log.debug("Declaration failed for " + entity + " named " + queueOrExchangeName
                        + " due to in equivalent arguments. Using the existing one.");
                log.debug(ex);
            }
            return newChannel;
        } else {
            throw ex;
        }
    }

    /**
     * Publish message in RPC style and wait for the response received replyTo queue
     *
     * @param exchangeName    the exchange to publish the message to
     * @param routingKey      the routing key
     * @param basicProperties other properties for the message
     * @param messageBody     the message body
     * @param timeout         waiting timeout until response receive
     * @return response message received to the replyTo queue
     * @throws IOException
     */
    private Delivery sendRPC(String exchangeName, String routingKey, AMQP.BasicProperties basicProperties,
                             byte[] messageBody, long timeout) throws IOException {
        Delivery response = null;
        String replyTo = basicProperties.getReplyTo();
        final BlockingQueue<Delivery> responses = new ArrayBlockingQueue<>(1);
        publishMessage(exchangeName, routingKey, basicProperties, messageBody);

        String replyConsumerTag = channel.basicConsume(replyTo, true, (consumerTag, delivery) ->
                responses.offer(delivery), consumerTag -> {
        });

        try {
            response = responses.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            channel.basicCancel(replyConsumerTag);
        }
        if (response == null) {
            throw new AxisFault("Did not receive a response within " + timeout + "ms to the replyTo queue " + replyTo);
        }
        return response;
    }

    /**
     * Publish a message and wait for confirmation. If a message nack'd by the broker, then throw an exception to the
     * caller.
     *
     * @param exchangeName    the exchange to publish the message to
     * @param routingKey      the routing key
     * @param basicProperties other properties for the message
     * @param messageBody     the message body
     * @param timeout         waiting timeout until confirmation receive
     * @throws IOException
     */
    private void sendPublisherConfirms(String exchangeName, String routingKey, AMQP.BasicProperties basicProperties,
                                       byte[] messageBody, long timeout) throws IOException, AxisRabbitMQException {
        long start = System.currentTimeMillis();
        publishMessage(exchangeName, routingKey, basicProperties, messageBody);
        //post-publish guard (rare race where timeout lands right after publish)
        if (Thread.currentThread().isInterrupted()) {
            log.warn("Thread Interrupted immediately AFTER publishing/BEFORE wait for confirm to the exchange: "
                    + exchangeName
                    + " with the routing key: " + routingKey + " Thread Name : " + Thread.currentThread().getName()
                    + " Thread Id : " + Thread.currentThread().getId()
                    + " Thread Group : " + Thread.currentThread().getThreadGroup());
            throw new AxisRabbitMQException(
                    "Thread : " + Thread.currentThread().getName() + " was interrupted immediately AFTER publish;" +
                            " cancelling confirms wait");
        }
        try {
            boolean success = channel.waitForConfirms(timeout);
            if (!success) {
                throw new AxisRabbitMQException("The message published to the exchange: " + exchangeName +
                        " with the routing key: " + routingKey + " nack'd by the broker.");
            }
        } catch (InterruptedException e) {
            log.warn("Thread Interrupted while waiting for publisher confirms  to the exchange: " + exchangeName
                    + " with the routing key: " + routingKey + " Thread Name : " + Thread.currentThread().getName()
                    + " Thread Id : " + Thread.currentThread().getId()
                    + " Thread Group : " + Thread.currentThread().getThreadGroup());
            Thread.currentThread().interrupt();
            long elapsed = System.currentTimeMillis() - start;
            throw new AxisRabbitMQException("Did not receive a confirmation within " + elapsed + "ms for the message " +
                    "published to the exchange: " + exchangeName + " with the routing key: " + routingKey
                    + "hence the thread was interrupted by timeout handler");
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - start;
            throw new AxisRabbitMQException("Did not receive a confirmation within " + elapsed + "ms for the message " +
                    "published to the exchange: " + exchangeName + " with the routing key: " + routingKey);
        }
    }

    /**
     * Perform basic publish
     *
     * @param exchangeName    the exchange to publish the message to
     * @param routingKey      the routing key
     * @param basicProperties other properties for the message
     * @param messageBody     the message body
     * @throws IOException
     */
    private void publishMessage(String exchangeName, String routingKey, AMQP.BasicProperties basicProperties,
                                byte[] messageBody) throws IOException {
        if (StringUtils.isNotEmpty(exchangeName)) {
            channel.basicPublish(exchangeName, routingKey, basicProperties, messageBody);
        } else {
            channel.basicPublish("", routingKey, basicProperties, messageBody);
        }
    }

    /**
     * Build and populate the AMQP.BasicProperties using the RabbitMQMessage
     *
     * @param msgCtx the {@link MessageContext} to be used to get the properties
     * @return AMQP.BasicProperties object
     */
    private AMQP.BasicProperties.Builder buildBasicProperties(MessageContext msgCtx)
            throws IOException {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties().builder();

        Map<String, Object> headers = (Map<String, Object>) msgCtx.getProperty(MessageContext.TRANSPORT_HEADERS);
        if (headers == null) {
            headers = new HashMap<>();
        }

        String timestamp = (String) msgCtx.getProperty(RabbitMQConstants.TIME_STAMP);
        if (StringUtils.isNotEmpty(timestamp)) {
            try {
                builder.timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp));
            } catch (ParseException e) {
                log.warn(timestamp + " can not be parsed as a java.util.Date");
            }
        }

        String expiration = (String) msgCtx.getProperty(RabbitMQConstants.EXPIRATION);
        if (expiration != null) {
            builder.expiration(expiration);
        }

        String userId = (String) msgCtx.getProperty(RabbitMQConstants.USER_ID);
        if (userId != null) {
            builder.userId(userId);
        }

        String appId = (String) msgCtx.getProperty(RabbitMQConstants.APP_ID);
        if (appId != null) {
            builder.appId(appId);
        }

        String clusterId = (String) msgCtx.getProperty(RabbitMQConstants.CLUSTER_ID);
        if (clusterId != null) {
            builder.clusterId(clusterId);
        }

        Integer priority = (Integer) msgCtx.getProperty(RabbitMQConstants.MSG_PRIORITY);
        if (priority != null) {
            builder.priority(priority);
        }

        String messageId = msgCtx.getMessageID();
        if (messageId != null) {
            builder.messageId(messageId);
        }

        String correlationId = (String) msgCtx.getProperty(RabbitMQConstants.CORRELATION_ID);
        if ((correlationId == null) || (correlationId.isEmpty())) {
            correlationId = messageId;
        }
        builder.correlationId(correlationId);

        String contentType = (String) msgCtx.getProperty(Constants.Configuration.MESSAGE_TYPE);
        if (contentType != null) {
            builder.contentType(contentType);
        }

        String contentEncoding = (String) msgCtx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING);
        if (contentEncoding != null) {
            builder.contentEncoding(contentEncoding);
        }

        String soapAction = msgCtx.getSoapAction();
        if (soapAction != null) {
            headers.put(RabbitMQConstants.SOAP_ACTION, soapAction);
        }

        if (senderType == RabbitMQSender.SenderType.RPC) {
            builder.replyTo(channel.queueDeclare().getQueue());
            headers.put(RabbitMQConstants.RABBITMQ_CON_FAC, factoryName);
        }

        if (msgCtx.getProperties().containsKey(BaseConstants.INTERNAL_TRANSACTION_COUNTED)) {
            headers.put(BaseConstants.INTERNAL_TRANSACTION_COUNTED,
                        msgCtx.getProperty(BaseConstants.INTERNAL_TRANSACTION_COUNTED));
        }

        builder.headers(headers);
        return builder;
    }

    private void invalidateChannel(RabbitMQSender.SenderType senderType, String factoryName, Channel channel) {
        try {
            log.warn("Channel returned to the pool is invalid. Hence, destroying the channel : " + channel);
            if (senderType == RabbitMQSender.SenderType.PUBLISHER_CONFIRMS) {
                rabbitMQConfirmChannelPool.invalidateObject(factoryName, channel);
            } else {
                rabbitMQChannelPool.invalidateObject(factoryName, channel);
            }

        } catch (Exception ex) {
            log.warn("Error occurred while returning a channel of " + factoryName + " back to the pool", ex);
        }
    }


}

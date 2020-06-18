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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.base.AbstractTransportSender;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.wso2.securevault.SecretResolver;

import java.util.HashMap;
import java.util.Map;

/**
 * The TransportSender for RabbitMQ AMQP Transport
 */
public class RabbitMQSender extends AbstractTransportSender {

    /**
     * The connection factory manager to be used when sending messages out
     */
    private RabbitMQConnectionFactory rabbitMQConnectionFactory;
    private RabbitMQChannelPool rabbitMQChannelPool;
    private RabbitMQChannelPool rabbitMQConfirmChannelPool;

    /**
     * Initialize the transport sender by reading pre-defined connection factories for
     * outgoing messages.
     *
     * @param cfgCtx       the configuration context
     * @param transportOut the transport sender definition from axis2.xml
     * @throws AxisFault on error
     */
    @Override
    public void init(ConfigurationContext cfgCtx, TransportOutDescription transportOut)
            throws AxisFault {
        try {
            super.init(cfgCtx, transportOut);
            SecretResolver secretResolver = cfgCtx.getAxisConfiguration().getSecretResolver();
            // initialize connection factory and pool
            rabbitMQConnectionFactory = new RabbitMQConnectionFactory();
            int poolSize =
                    RabbitMQUtils.resolveTransportDescription(transportOut, secretResolver, rabbitMQConnectionFactory);
            RabbitMQConnectionPool rabbitMQConnectionPool = new RabbitMQConnectionPool(rabbitMQConnectionFactory, poolSize);
            // initialize channel factory and pool
            RabbitMQChannelFactory rabbitMQChannelFactory = new RabbitMQChannelFactory(rabbitMQConnectionPool);
            rabbitMQChannelPool = new RabbitMQChannelPool(rabbitMQChannelFactory, poolSize);
            // initialize confirm channel factory and pool
            RabbitMQConfirmChannelFactory rabbitMQConfirmChannelFactory =
                    new RabbitMQConfirmChannelFactory(rabbitMQConnectionPool);
            rabbitMQConfirmChannelPool = new RabbitMQChannelPool(rabbitMQConfirmChannelFactory, poolSize);
            log.info("RabbitMQ AMQP Transport Sender initialized...");
        } catch (AxisRabbitMQException e) {
            throw new AxisFault("Error occurred while initializing the RabbitMQ AMQP Transport Sender.", e);
        }

    }

    /**
     * Stop the sender
     */
    @Override
    public void stop() {
        super.stop();
        log.info("RabbitMQ AMQP Transport Sender stopped...");
    }

    /**
     * Performs the sending of the AMQP message
     *
     * @param msgCtx           the axis2 message context
     * @param targetEPR        the RabbitMQ endpoint
     * @param outTransportInfo the {@link OutTransportInfo} object
     * @throws AxisFault
     */
    @Override
    public void sendMessage(MessageContext msgCtx, String targetEPR,
                            OutTransportInfo outTransportInfo) throws AxisFault {
        if (targetEPR != null) {
            // execute when publishing a message to queue in standard flow
            RabbitMQOutTransportInfo transportOutInfo = new RabbitMQOutTransportInfo(targetEPR);
            String factoryName = RabbitMQUtils
                    .resolveTransportDescriptionFromTargetEPR(transportOutInfo.getProperties(),
                            rabbitMQConnectionFactory);
            Channel channel = null;
            Delivery response = null;
            SenderType senderType = null;
            try {
                // get rabbitmq properties from the EPR
                Map<String, String> epProperties = BaseUtils.getEPRProperties(targetEPR);

                // set the routing key
                String queueName = epProperties.get(RabbitMQConstants.QUEUE_NAME);
                String routingKey = epProperties.get(RabbitMQConstants.QUEUE_ROUTING_KEY);
                if (StringUtils.isNotEmpty(queueName) && StringUtils.isEmpty(routingKey)) {
                    routingKey = queueName; // support the backward compatibility
                } else if (StringUtils.isEmpty(queueName) && StringUtils.isEmpty(routingKey)) {
                    routingKey = targetEPR.substring(targetEPR.indexOf("/") + 1, targetEPR.indexOf("?"));
                }

                // send the message
                senderType = getSenderType(msgCtx, epProperties);
                if (senderType == SenderType.PUBLISHER_CONFIRMS) {
                    channel = rabbitMQConfirmChannelPool.borrowObject(factoryName);
                } else {
                    channel = rabbitMQChannelPool.borrowObject(factoryName);
                }
                RabbitMQMessageSender sender = new RabbitMQMessageSender(channel, factoryName, senderType);
                response = sender.send(routingKey, msgCtx, epProperties);
            } catch (Exception e) {
                log.error("Error occurred while sending message out.", e);
                channel = null;
            } finally {
                returnToPool(factoryName, channel, senderType);
            }

            // inject message to the axis engine if a response received
            if (response != null) {
                MessageContext responseMsgCtx = createResponseMessageContext(msgCtx);
                String contentType = RabbitMQUtils.buildMessageWithReplyTo(
                        response.getProperties(), response.getBody(), responseMsgCtx);
                handleIncomingMessage(responseMsgCtx, RabbitMQUtils.getTransportHeaders(response.getProperties()),
                        RabbitMQUtils.getSoapAction(response.getProperties()),
                        contentType);
            }
        } else if (outTransportInfo instanceof RabbitMQOutTransportInfo) {
            // execute when publishing a message to the replyTo queue in request-response flow
            RabbitMQOutTransportInfo transportOutInfo = (RabbitMQOutTransportInfo) outTransportInfo;
            String factoryName = transportOutInfo.getConnectionFactoryName();
            Channel channel = null;
            try {
                // get the correlationId and replyTo queue from the transport headers
                Map<String, Object> transportHeaders =
                        (Map<String, Object>) msgCtx.getProperty(msgCtx.TRANSPORT_HEADERS);
                msgCtx.setProperty(
                        RabbitMQConstants.CORRELATION_ID, transportHeaders.get(RabbitMQConstants.CORRELATION_ID));
                String replyTo = (String) transportHeaders.get(RabbitMQConstants.RABBITMQ_REPLY_TO);

                // set rabbitmq properties
                Map<String, String> rabbitMQProperties = new HashMap<>();
                rabbitMQProperties.put(RabbitMQConstants.QUEUE_AUTODECLARE, "false");
                rabbitMQProperties.put(RabbitMQConstants.EXCHANGE_AUTODECLARE, "false");

                // send the message to the replyTo queue
                channel = rabbitMQChannelPool.borrowObject(factoryName);
                RabbitMQMessageSender sender = new RabbitMQMessageSender(channel, factoryName, SenderType.DEFAULT);
                sender.send(replyTo, msgCtx, rabbitMQProperties);
            } catch (Exception e) {
                log.error("Error occurred while sending message out.", e);
                channel = null;
            } finally {
                returnToPool(factoryName, channel, SenderType.DEFAULT);
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
    private void returnToPool(String factoryName, Channel channel, SenderType senderType) {
        if (senderType == SenderType.PUBLISHER_CONFIRMS) {
            rabbitMQConfirmChannelPool.returnObject(factoryName, channel);
        } else {
            rabbitMQChannelPool.returnObject(factoryName, channel);
        }
    }

    /**
     * The different sender types
     */
    enum SenderType {
        RPC,
        PUBLISHER_CONFIRMS,
        DEFAULT;
    }

    /**
     * Get the type of the sender to execute the relevant logic
     *
     * @param messageContext the {@link MessageContext} object
     * @param epProperties   the endpoint properties
     * @return the {@link SenderType}
     */
    private SenderType getSenderType(MessageContext messageContext, Map<String, String> epProperties) {
        SenderType type;
        if (waitForSynchronousResponse(messageContext)) {
            type = SenderType.RPC;
        } else if (BooleanUtils.toBoolean(epProperties.get(RabbitMQConstants.PUBLISHER_CONFIRMS_ENABLED))) {
            type = SenderType.PUBLISHER_CONFIRMS;
        } else {
            type = SenderType.DEFAULT;
        }
        return type;
    }
}

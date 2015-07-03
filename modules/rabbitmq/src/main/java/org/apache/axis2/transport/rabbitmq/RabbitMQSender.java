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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.base.AbstractTransportSender;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.transport.rabbitmq.utils.AxisRabbitMQException;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQConstants;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

/**
 * The TransportSender for RabbitMQ AMQP Transport
 */
public class RabbitMQSender extends AbstractTransportSender {

    /**
     * The connection factory manager to be used when sending messages out
     */
    private RabbitMQConnectionFactoryManager rabbitMQConnectionFactoryManager;

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
        super.init(cfgCtx, transportOut);
        rabbitMQConnectionFactoryManager = new RabbitMQConnectionFactoryManager(transportOut);
        log.info("RabbitMQ AMQP Transport Sender initialized...");

    }

    @Override
    public void stop() {
        // clean up senders connection factory, connections
        rabbitMQConnectionFactoryManager.stop();
        super.stop();
    }

    /**
     * Performs the sending of the RabbitMQ AMQP message
     */
    @Override
    public void sendMessage(MessageContext msgCtx, String targetEPR,
                            OutTransportInfo outTransportInfo) throws AxisFault {
        if (targetEPR != null) {
            RabbitMQOutTransportInfo transportOutInfo = new RabbitMQOutTransportInfo(targetEPR);
            RabbitMQConnectionFactory factory = getConnectionFactory(transportOutInfo);
            if (factory != null) {
                RabbitMQMessageSender sender = new RabbitMQMessageSender(factory, targetEPR);
                sendOverAMQP(factory, msgCtx, sender, targetEPR);
            }
        }
    }

    /**
     * Perform actual sending of the AMQP message
     */
    private void sendOverAMQP(RabbitMQConnectionFactory factory, MessageContext msgContext, RabbitMQMessageSender sender, String targetEPR)
            throws AxisFault {
        try {
            RabbitMQMessage message = new RabbitMQMessage(msgContext);
            sender.send(message, msgContext);

            if (message.getReplyTo() != null) {
                processResponse(factory, msgContext, message.getCorrelationId(), message.getReplyTo(), BaseUtils.getEPRProperties(targetEPR));
            }

        } catch (AxisRabbitMQException e) {
            handleException("Error occurred while sending message out", e);
        } catch (IOException e) {
            handleException("Error occurred while sending message out", e);
        }
    }

    private void processResponse(RabbitMQConnectionFactory factory, MessageContext msgContext, String correlationID, String replyTo, Hashtable<String, String> eprProperties) throws IOException {

        Connection connection = factory.createConnection();

        if (!RabbitMQUtils.isQueueAvailable(connection, replyTo)) {
            handleException("Reply-to queue : " + replyTo + " not available.");
        }

        Channel channel = connection.createChannel();
        QueueingConsumer consumer = new QueueingConsumer(channel);
        QueueingConsumer.Delivery delivery = null;
        RabbitMQMessage message = new RabbitMQMessage();
        boolean responseFound = false;

        int timeout = RabbitMQConstants.DEFAULT_REPLY_TO_TIMEOUT;
        String timeoutStr = eprProperties.get(RabbitMQConstants.REPLY_TO_TIMEOUT);
        if (!StringUtils.isEmpty(timeoutStr)) {
            try {
                timeout = Integer.parseInt(timeoutStr);
            } catch (NumberFormatException e) {
                log.warn("Number format error in reading replyto timeout value. Proceeding with default value (10000ms)", e);
            }
        }

        //start consuming without acknowledging
        String consumerTag = channel.basicConsume(replyTo, false, consumer);

        try {
            while (!responseFound) {
                log.debug("Waiting for next delivery from reply to queue " + replyTo);
                delivery = consumer.nextDelivery(timeout);
                if (delivery != null) {
                    if (delivery.getProperties().getCorrelationId().equals(correlationID)) {
                        responseFound = true;
                        log.debug("Found matching response with correlation ID : " + correlationID + ". Sending ack");
                        //acknowledge correct message
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } else {
                        //not acknowledge wrong messages and re-queue
                        log.debug("Found messages with wrong correlation ID. Re-queueing and sending nack");
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    }
                }
            }
        } catch (ShutdownSignalException e) {
            log.error("Error receiving message from RabbitMQ broker " + e.getLocalizedMessage());
        } catch (InterruptedException e) {
            log.error("Error receiving message from RabbitMQ broker " + e.getLocalizedMessage());
        } catch (ConsumerCancelledException e) {
            log.error("Error receiving message from RabbitMQ broker" + e.getLocalizedMessage());
        } finally {
            if (channel != null || channel.isOpen()) {
                //stop consuming
                channel.basicCancel(consumerTag);
            }
        }

        if (delivery != null) {
            log.debug("Processing response from reply-to queue");
            AMQP.BasicProperties properties = delivery.getProperties();
            Map<String, Object> headers = properties.getHeaders();
            message.setBody(delivery.getBody());
            message.setDeliveryTag(delivery.getEnvelope().getDeliveryTag());
            message.setReplyTo(properties.getReplyTo());
            message.setMessageId(properties.getMessageId());

            //get content type from message
            String contentType = properties.getContentType();
            if (contentType == null) {
                //if not get content type from transport parameter
                contentType = eprProperties.get(RabbitMQConstants.REPLY_TO_CONTENT_TYPE);
                if (contentType == null) {
                    //if none is given, set to default content type
                    log.warn("Setting default content type " + RabbitMQConstants.DEFAULT_CONTENT_TYPE);
                    contentType = RabbitMQConstants.DEFAULT_CONTENT_TYPE;
                }
            }
            message.setContentType(contentType);
            message.setContentEncoding(properties.getContentEncoding());
            message.setCorrelationId(properties.getCorrelationId());

            if (headers != null) {
                message.setHeaders(headers);
                if (headers.get(RabbitMQConstants.SOAP_ACTION) != null) {
                    message.setSoapAction(headers.get(
                            RabbitMQConstants.SOAP_ACTION).toString());
                }
            }

            MessageContext responseMsgCtx = createResponseMessageContext(msgContext);
            RabbitMQUtils.setSOAPEnvelope(message, responseMsgCtx, contentType);
            handleIncomingMessage(responseMsgCtx, RabbitMQUtils.getTransportHeaders(message),
                    message.getSoapAction(), message.getContentType());
        }
    }

    /**
     * Get corresponding AMQP connection factory defined within the transport sender for the
     * transport-out information - usually constructed from a targetEPR
     *
     * @param transportInfo the transport-out information
     * @return the corresponding ConnectionFactory, if any
     */
    private RabbitMQConnectionFactory getConnectionFactory(RabbitMQOutTransportInfo transportInfo) {
        Hashtable<String, String> props = transportInfo.getProperties();
        RabbitMQConnectionFactory factory = rabbitMQConnectionFactoryManager.getConnectionFactory(props);
        return factory;
    }
}

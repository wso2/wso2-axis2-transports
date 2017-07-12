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

package org.apache.axis2.transport.rabbitmq.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.transport.rabbitmq.RabbitMQConnectionFactory;
import org.apache.axis2.transport.rabbitmq.RabbitMQMessage;
import org.apache.axis2.transport.rabbitmq.utils.AxisRabbitMQException;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQConstants;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQUtils;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;

/**
 * Class that performs the actual sending of a RabbitMQ AMQP message,
 */

public class RabbitMQRPCMessageSender {
    private static final Log log = LogFactory.getLog(RabbitMQRPCMessageSender.class);

    private DualChannel dualChannel = null;
    private String targetEPR = null;
    private Hashtable<String, String> epProperties;
    private RabbitMQConnectionFactory connectionFactory;

    /**
     * Create a RabbitMQSender using a ConnectionFactory and target EPR
     *
     * @param connectionFactory the RabbitMQ connection factory
     * @param targetEPR         the targetAddress
     * @param epProperties
     */
    //TODO : cache connection factories with targetEPR string. should include queue autodeclare properties etc..
    public RabbitMQRPCMessageSender(RabbitMQConnectionFactory connectionFactory, String targetEPR, Hashtable<String, String> epProperties) {

        this.targetEPR = targetEPR;
        this.connectionFactory = connectionFactory;

        try {
            dualChannel = connectionFactory.getRPCChannel();
        } catch (InterruptedException e) {
            handleException("Error while getting RPC channel", e);
        }

        if (!this.targetEPR.startsWith(RabbitMQConstants.RABBITMQ_PREFIX)) {
            handleException("Invalid prefix for a AMQP EPR : " + targetEPR);
        } else {
            this.epProperties = epProperties;
        }
    }

    public RabbitMQMessage send(RabbitMQMessage message, MessageContext msgContext) throws
            AxisRabbitMQException, IOException {

        publish(message, msgContext);
        RabbitMQMessage responseMessage = processResponse(message.getCorrelationId());

        //release the dual channel to the pool
        try {
            connectionFactory.pushRPCChannel(dualChannel);
        } catch (InterruptedException e) {
            handleException(e.getMessage());
        }

        return responseMessage;
    }

    /**
     * Perform the creation of exchange/queue and the Outputstream
     *
     * @param message    the RabbitMQ AMQP message
     * @param msgContext the Axis2 MessageContext
     */
    private void publish(RabbitMQMessage message, MessageContext msgContext) throws
            AxisRabbitMQException, IOException {

        String exchangeName = null;
        AMQP.BasicProperties basicProperties = null;
        byte[] messageBody = null;

        if (dualChannel.isOpen()) {
            String queueName = epProperties.get(RabbitMQConstants.QUEUE_NAME);
            String routeKey = epProperties
                    .get(RabbitMQConstants.QUEUE_ROUTING_KEY);
            exchangeName = epProperties.get(RabbitMQConstants.EXCHANGE_NAME);
            String exchangeType = epProperties
                    .get(RabbitMQConstants.EXCHANGE_TYPE);
            String correlationID = epProperties.get(RabbitMQConstants.CORRELATION_ID);
            String replyTo = dualChannel.getReplyToQueue();

            String queueAutoDeclareStr = epProperties.get(RabbitMQConstants.QUEUE_AUTODECLARE);
            String exchangeAutoDeclareStr = epProperties.get(RabbitMQConstants.EXCHANGE_AUTODECLARE);
            boolean queueAutoDeclare = true;
            boolean exchangeAutoDeclare = true;

            if (!StringUtils.isEmpty(queueAutoDeclareStr)) {
                queueAutoDeclare = Boolean.parseBoolean(queueAutoDeclareStr);
            }

            if (!StringUtils.isEmpty(exchangeAutoDeclareStr)) {
                exchangeAutoDeclare = Boolean.parseBoolean(exchangeAutoDeclareStr);
            }

            message.setReplyTo(replyTo);

            if ((!StringUtils.isEmpty(replyTo)) && (StringUtils.isEmpty(correlationID))) {
                //if reply-to is enabled a correlationID must be available. If not specified, use messageID
                correlationID = message.getMessageId();
            }

            if (!StringUtils.isEmpty(correlationID)) {
                message.setCorrelationId(correlationID);
            }

            if (queueName == null || queueName.equals("")) {
                log.info("No queue name is specified");
            }

            if (routeKey == null && !"x-consistent-hash".equals(exchangeType)) {
                if (queueName == null || queueName.equals("")) {
                    log.info("Routing key is not specified");
                } else {
                    log.info(
                            "Routing key is not specified. Using queue name as the routing key.");
                    routeKey = queueName;
                }
            }

            //Declaring the queue
            if (queueAutoDeclare && queueName != null && !queueName.equals("")) {
                //get channel with dualChannel.getChannel() since it will create a new channel if channel is closed
                RabbitMQUtils.declareQueue(dualChannel, queueName, epProperties);
            }

            //Declaring the exchange
            if (exchangeAutoDeclare && exchangeName != null && !exchangeName.equals("")) {
                RabbitMQUtils.declareExchange(dualChannel, exchangeName, epProperties);

                if (queueName != null && !"x-consistent-hash".equals(exchangeType)) {
                    // Create bind between the queue and exchange with the routeKey
                    try {
                        dualChannel.getChannel().queueBind(queueName, exchangeName, routeKey);
                    } catch (IOException e) {
                        handleException(
                                "Error occurred while creating the bind between the queue: "
                                        + queueName + " & exchange: " + exchangeName + " with route-key " + routeKey, e);
                    }
                }
            }

            //build basic properties from message
            AMQP.BasicProperties.Builder builder = buildBasicProperties(message);

            String deliveryModeString = epProperties
                    .get(RabbitMQConstants.QUEUE_DELIVERY_MODE);
            int deliveryMode = RabbitMQConstants.DEFAULT_DELIVERY_MODE;
            if (deliveryModeString != null) {
                deliveryMode = Integer.parseInt(deliveryModeString);
            }

            //TODO : override properties from message with ones from transport properties
            //set builder properties from transport properties (overrides current properties)
            builder.deliveryMode(deliveryMode);
            builder.replyTo(replyTo);

            basicProperties = builder.build();
            OMOutputFormat format = BaseUtils.getOMOutputFormat(msgContext);
            MessageFormatter messageFormatter = null;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                messageFormatter = MessageProcessorSelector.getMessageFormatter(msgContext);
            } catch (AxisFault axisFault) {
                throw new AxisRabbitMQException(
                        "Unable to get the message formatter to use",
                        axisFault);
            }

            //server plugging should be enabled before using x-consistent hashing
            //for x-consistent-hashing only exchangeName, exchangeType and routingKey should be
            // given. Queue/exchange creation, bindings should be done at the broker
            try {
                // generate random value as routeKey if the exchangeType is
                // x-consistent-hash type
                if (exchangeType != null
                        && exchangeType.equals("x-consistent-hash")) {
                    routeKey = UUID.randomUUID().toString();
                }

            } catch (UnsupportedCharsetException ex) {
                handleException(
                        "Unsupported encoding "
                                + format.getCharSetEncoding(), ex);
            }
            try {
                messageFormatter.writeTo(msgContext, format, out, false);
                messageBody = out.toByteArray();
            } catch (IOException e) {
                handleException("IO Error while creating BytesMessage", e);
            } finally {
                if (out != null) {
                    out.close();
                }
            }

            try {
                if (exchangeName != null && exchangeName != "") {
                    if (log.isDebugEnabled()) {
                        log.debug("Publishing message to exchange " + exchangeName + " with route key " + routeKey);
                    }
                    dualChannel.getChannel().basicPublish(exchangeName, routeKey, basicProperties,
                            messageBody);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Publishing message with route key " + routeKey);
                    }
                    dualChannel.getChannel().basicPublish("", routeKey, basicProperties,
                            messageBody);
                }
            } catch (IOException e) {
                handleException("Error while publishing the message", e);
            }
        } else {
            handleException("Channel cannot be created");
        }
    }


    private RabbitMQMessage processResponse(String correlationID) throws IOException {

        QueueingConsumer consumer = dualChannel.getConsumer();
        QueueingConsumer.Delivery delivery = null;
        RabbitMQMessage message = new RabbitMQMessage();
        String replyToQueue = dualChannel.getReplyToQueue();

        String queueAutoDeclareStr = epProperties.get(RabbitMQConstants.QUEUE_AUTODECLARE);
        boolean queueAutoDeclare = true;

        if (!StringUtils.isEmpty(queueAutoDeclareStr)) {
            queueAutoDeclare = Boolean.parseBoolean(queueAutoDeclareStr);
        }

        if (queueAutoDeclare && !RabbitMQUtils.isQueueAvailable(dualChannel.getChannel(), replyToQueue)) {
            log.info("Reply-to queue : " + replyToQueue + " not available, hence creating a new one");
            RabbitMQUtils.declareQueue(dualChannel, replyToQueue, epProperties);
        }

        int timeout = RabbitMQConstants.DEFAULT_REPLY_TO_TIMEOUT;
        String timeoutStr = epProperties.get(RabbitMQConstants.REPLY_TO_TIMEOUT);
        if (!StringUtils.isEmpty(timeoutStr)) {
            try {
                timeout = Integer.parseInt(timeoutStr);
            } catch (NumberFormatException e) {
                log.warn("Number format error in reading replyto timeout value. Proceeding with default value (30000ms)", e);
            }
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug("Waiting for delivery from reply to queue " + replyToQueue + " corr id : " + correlationID);
            }
            delivery = consumer.nextDelivery(timeout);
            if (delivery != null) {
                if (!StringUtils.isEmpty(delivery.getProperties().getCorrelationId())) {
                    if (delivery.getProperties().getCorrelationId().equals(correlationID)) {
                        if(log.isDebugEnabled()) {
                            log.debug("Found matching response with correlation ID : " + correlationID + ".");
                        }
                    } else {
                        log.error("Response not queued in " + replyToQueue + " for correlation ID : " + correlationID);
                        return null;
                    }
                }
            } else {
                log.error("Response not queued in " + replyToQueue);
            }
        } catch (ShutdownSignalException e) {
            log.error("Error receiving message from RabbitMQ broker " + e.getLocalizedMessage());
        } catch (InterruptedException e) {
            log.error("Error receiving message from RabbitMQ broker " + e.getLocalizedMessage());
        } catch (ConsumerCancelledException e) {
            log.error("Error receiving message from RabbitMQ broker" + e.getLocalizedMessage());
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
                contentType = epProperties.get(RabbitMQConstants.REPLY_TO_CONTENT_TYPE);
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
        }
        return message;
    }


    /**
     * Build and populate the AMQP.BasicProperties using the RabbitMQMessage
     *
     * @param message the RabbitMQMessage to be used to get the properties
     * @return AMQP.BasicProperties object
     */
    private AMQP.BasicProperties.Builder buildBasicProperties(RabbitMQMessage message) {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties().builder();
        builder.messageId(message.getMessageId());
        builder.contentType(message.getContentType());
        builder.replyTo(message.getReplyTo());
        builder.correlationId(message.getCorrelationId());
        builder.contentEncoding(message.getContentEncoding());
        Map<String, Object> headers = message.getHeaders();
        if (message.getSoapAction() != null) {
            headers.put(RabbitMQConstants.SOAP_ACTION, message.getSoapAction());
        }
        builder.headers(headers);
        return builder;
    }

    private void handleException(String s) {
        log.error(s);
        throw new AxisRabbitMQException(s);
    }

    private void handleException(String message, Exception e) {
        log.error(message, e);
        throw new AxisRabbitMQException(message, e);
    }

}
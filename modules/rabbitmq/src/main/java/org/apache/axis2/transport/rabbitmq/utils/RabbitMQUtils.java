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


package org.apache.axis2.transport.rabbitmq.utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.rabbitmq.RMQChannel;
import org.apache.axis2.transport.rabbitmq.RabbitMQMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;


public class RabbitMQUtils {

    private static final Log log = LogFactory.getLog(RabbitMQUtils.class);

    public static Connection createConnection(ConnectionFactory factory) throws IOException {
        Connection connection = factory.newConnection();
        return connection;
    }

    public static String getProperty(MessageContext mc, String key) {
        return (String) mc.getProperty(key);
    }

    public static void setSOAPEnvelope(RabbitMQMessage message, MessageContext msgContext,
                                       String contentType) throws AxisFault {

        int index = contentType.indexOf(';');
        String type = index > 0 ? contentType.substring(0, index) : contentType;
        Builder builder = BuilderUtil.getBuilderFromSelector(type, msgContext);
        if (builder == null) {
            if (log.isDebugEnabled()) {
                log.debug("No message builder found for type '" + type + "'. Falling back to SOAP.");
            }
            builder = new SOAPBuilder();
        }

        OMElement documentElement;
        String charSetEnc = null;
        try {
            if (contentType != null) {
                charSetEnc = new ContentType(contentType).getParameter("charset");
            }
        } catch (ParseException ex) {
            log.debug("Parse error", ex);
        }
        msgContext.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, charSetEnc);

        documentElement = builder.processDocument(
                new ByteArrayInputStream(message.getBody()), contentType,
                msgContext);


        msgContext.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
    }

    public static String getSOAPActionHeader(RabbitMQMessage message) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public static Map getTransportHeaders(RabbitMQMessage message) {
        Map<String, Object> map = new HashMap<String, Object>();

        // correlation ID
        if (message.getCorrelationId() != null) {
            map.put(RabbitMQConstants.CORRELATION_ID, message.getCorrelationId());
        }

        // if a AMQP message ID is found
        if (message.getMessageId() != null) {
            map.put(RabbitMQConstants.MESSAGE_ID, message.getMessageId());
        }

        // replyto destination name
        if (message.getReplyTo() != null) {
            String dest = message.getReplyTo();
            map.put(RabbitMQConstants.RABBITMQ_REPLY_TO, dest);
        }

        // any other transport properties / headers
        Map<String, Object> headers = message.getHeaders();
        if (headers != null && !headers.isEmpty()) {
            for (String headerName : headers.keySet()) {
                map.put(headerName, headers.get(headerName));
            }
        }

        return map;
    }


    public static boolean isDurableQueue(Hashtable<String, String> properties) {
        String durable = properties.get(RabbitMQConstants.QUEUE_DURABLE);
        return durable != null && Boolean.parseBoolean(durable);
    }

    public static boolean isExclusiveQueue(Hashtable<String, String> properties) {
        String exclusive = properties.get(RabbitMQConstants.QUEUE_EXCLUSIVE);
        return exclusive != null && Boolean.parseBoolean(exclusive);
    }

    public static boolean isAutoDeleteQueue(Hashtable<String, String> properties) {
        String autoDelete = properties.get(RabbitMQConstants.QUEUE_AUTO_DELETE);
        return autoDelete != null && Boolean.parseBoolean(autoDelete);
    }

    public static boolean isQueueAvailable(Channel channel, String queueName) throws IOException {
        try {
            // check availability of the named queue
            // if an error is encountered, including if the queue does not exist and if the
            // queue is exclusively owned by another connection
            channel.queueDeclarePassive(queueName);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static void declareQueue(RMQChannel rmqChannel, String queueName, boolean isDurable,
                                    boolean isExclusive, boolean isAutoDelete) throws IOException {

        boolean queueAvailable = isQueueAvailable(rmqChannel.getChannel(), queueName);

        if (!queueAvailable) {
            if (log.isDebugEnabled()) {
                log.debug("Queue :" + queueName + " not found or already declared exclusive. Declaring the queue.");
            }

            // Declare the named queue if it does not exists.
            try {
                rmqChannel.getChannel().queueDeclare(queueName, isDurable, isExclusive, isAutoDelete, null);
            } catch (IOException e) {
                handleException("Error while creating queue: " + queueName, e);
            }
        }
    }

    public static void declareQueue(RMQChannel rmqChannel, String queueName,
                                    Hashtable<String, String> properties) throws IOException {

        Channel channel = rmqChannel.getChannel();
        Boolean queueAvailable = isQueueAvailable(channel, queueName);

        if (!queueAvailable) {
            try {
                rmqChannel.getChannel().queueDeclare(queueName, isDurableQueue(properties),
                        isExclusiveQueue(properties), isAutoDeleteQueue(properties), null);

            } catch (IOException e) {
                handleException("Error while creating queue: " + queueName, e);
            }
        }
    }

    public static void declareExchange(RMQChannel rmqChannel, String exchangeName, Hashtable<String, String> properties) throws IOException {
        Boolean exchangeAvailable = false;

        String exchangeType = properties
                .get(RabbitMQConstants.EXCHANGE_TYPE);
        String durable = properties.get(RabbitMQConstants.EXCHANGE_DURABLE);
        try {
            // check availability of the named exchange.
            // The server will raise an IOException
            // if the named exchange already exists.
            rmqChannel.getChannel().exchangeDeclarePassive(exchangeName);
            exchangeAvailable = true;
        } catch (IOException e) {
            log.info("Exchange :" + exchangeName + " not found.Declaring exchange.");
        }

        if (!exchangeAvailable) {
            // Declare the named exchange if it does not exists.
            try {
                if (exchangeType != null
                        && !exchangeType.equals("")) {
                    if (durable != null && !durable.equals("")) {
                        rmqChannel.getChannel().exchangeDeclare(exchangeName,
                                exchangeType,
                                Boolean.parseBoolean(durable));
                    } else {
                        rmqChannel.getChannel().exchangeDeclare(exchangeName,
                                exchangeType, true);
                    }
                } else {
                    rmqChannel.getChannel().exchangeDeclare(exchangeName, "direct", true);
                }
            } catch (IOException e) {
                handleException("Error occurred while declaring exchange.", e);
            }
        }
    }

    public static void handleException(String message, Exception e) {
        log.error(message, e);
        throw new AxisRabbitMQException(message, e);
    }

}

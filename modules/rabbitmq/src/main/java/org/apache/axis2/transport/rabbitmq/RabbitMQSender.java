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

import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.base.AbstractTransportSender;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.transport.rabbitmq.rpc.RabbitMQRPCMessageSender;
import org.apache.axis2.transport.rabbitmq.utils.AxisRabbitMQException;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQConstants;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQUtils;
import org.apache.commons.lang.StringUtils;
import org.wso2.securevault.SecretResolver;

import java.io.IOException;
import java.util.Hashtable;

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
        SecretResolver secretResolver = cfgCtx.getAxisConfiguration().getSecretResolver();
        rabbitMQConnectionFactoryManager = new RabbitMQConnectionFactoryManager(transportOut, secretResolver);
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
                sendOverAMQP(factory, msgCtx, targetEPR);
            }
        }
    }

    /**
     * Perform actual sending of the AMQP message
     */
    private void sendOverAMQP(RabbitMQConnectionFactory factory, MessageContext msgContext, String targetEPR)
            throws AxisFault {
        try {
            RabbitMQMessage message = new RabbitMQMessage(msgContext);
            Hashtable<String, String> epProperties = BaseUtils.getEPRProperties(targetEPR);

            if (!StringUtils.isEmpty(epProperties.get(RabbitMQConstants.REPLY_TO_NAME))) {
                // request-response scenario
                RabbitMQRPCMessageSender sender = new RabbitMQRPCMessageSender(factory, targetEPR, epProperties);
                RabbitMQMessage responseMessage = sender.send(message, msgContext);
                MessageContext responseMsgCtx = createResponseMessageContext(msgContext);
                RabbitMQUtils.setSOAPEnvelope(responseMessage, responseMsgCtx, responseMessage.getContentType());
                handleIncomingMessage(responseMsgCtx, RabbitMQUtils.getTransportHeaders(responseMessage),
                        responseMessage.getSoapAction(), responseMessage.getContentType());
            } else {
                //Basic out only publish
                RabbitMQMessageSender sender = new RabbitMQMessageSender(factory, targetEPR, epProperties);
                sender.send(message, msgContext);
            }

        } catch (AxisRabbitMQException e) {
            handleException("Error occurred while sending message out", e);
        } catch (IOException e) {
            handleException("Error occurred while sending message out", e);
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

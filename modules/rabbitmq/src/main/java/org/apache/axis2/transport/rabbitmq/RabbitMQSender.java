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
import org.apache.axis2.transport.rabbitmq.utils.AxisRabbitMQException;

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
        rabbitMQConnectionFactoryManager = new RabbitMQConnectionFactoryManager(transportOut);
        log.info("RabbitMQ AMQP Transport Sender initialized...");

    }

    @Override
    public void stop(){
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

        RabbitMQConnectionFactory factory = null;
        RabbitMQMessageSender sender = null;
        RabbitMQOutTransportInfo transportOutInfo = null;

        if (targetEPR != null) {
            transportOutInfo = new RabbitMQOutTransportInfo(targetEPR);
            factory = getAMQPConnectionFactory(transportOutInfo);

            if (factory != null) {
                sender = new RabbitMQMessageSender(factory, targetEPR);
                sendOverAMQP(msgCtx, sender);
            }
        }
    }

    /**
     * Perform actual sending of the AMQP message
     */

    private void sendOverAMQP(MessageContext msgContext, RabbitMQMessageSender sender)
            throws AxisFault {
        try {
            RabbitMQMessage message = new RabbitMQMessage(msgContext);
            sender.send(message, msgContext);
        } catch (AxisRabbitMQException e) {
            handleException("Error occured while sending message out", e);
        }
//TODO : if exception is thrown, it should invoke the fault sequece. Test that.
    }

    /**
     * Get corresponding AMQP connection factory defined within the transport sender for the
     * transport-out information - usually constructed from a targetEPR
     *
     * @param transportInfo the transport-out information
     * @return the corresponding ConnectionFactory, if any
     */
    private RabbitMQConnectionFactory getAMQPConnectionFactory(RabbitMQOutTransportInfo transportInfo) {
        Hashtable<String, String> props = transportInfo.getProperties();
        RabbitMQConnectionFactory factory = rabbitMQConnectionFactoryManager.getAMQPConnectionFactory(props);
        return factory;
    }
}

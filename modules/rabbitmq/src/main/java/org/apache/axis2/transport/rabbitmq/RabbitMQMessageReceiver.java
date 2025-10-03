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
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.base.AckDecision;
import org.apache.axis2.transport.base.AckDecisionCallback;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * This is the RabbitMQ AMQP message receiver which is invoked when a message is received. This processes
 * the message through the axis2 engine
 */
public class RabbitMQMessageReceiver {
    private static final Log log = LogFactory.getLog(RabbitMQMessageReceiver.class);
    private final RabbitMQEndpoint endpoint;
    private final RabbitMQListener listener;

    /**
     * Create a new RabbitMQMessage receiver
     *
     * @param listener the AMQP transport Listener
     * @param endpoint the RabbitMQEndpoint definition to be used
     */
    public RabbitMQMessageReceiver(RabbitMQListener listener, RabbitMQEndpoint endpoint) {
        this.endpoint = endpoint;
        this.listener = listener;
    }

    /**
     * Process the new message through Axis2
     *
     * @param messageProperties the AMQP basic messageProperties
     * @param body       the message body
     * @return true if no mediation errors
     * @throws AxisFault on Axis2 errors
     */
    AcknowledgementMode processThroughAxisEngine(AMQP.BasicProperties messageProperties, byte[] body) {
        try {
            MessageContext msgContext = endpoint.createMessageContext();
            Map<String, String> serviceProperties = endpoint.getServiceTaskManager().getRabbitMQProperties();
            String contentType = RabbitMQUtils.buildMessage(messageProperties, body, msgContext, serviceProperties);

            AckDecisionCallback ackDecisionCallback = null;
            if (RabbitMQAckConfig.isCallbackControlledAckEnabled()) {
                // Store an AckDecisionCallback in the MessageContext so downstream mediators can signal
                // how the message should be acknowledged (ack / requeue) by setting related properties.
                ackDecisionCallback = new AckDecisionCallback();
                msgContext.setProperty(RabbitMQConstants.ACKNOWLEDGEMENT_DECISION, ackDecisionCallback);
            }
            listener.handleIncomingMessage(msgContext, RabbitMQUtils.getTransportHeaders(messageProperties),
                                           RabbitMQUtils.getSoapAction(messageProperties), contentType);
            if (RabbitMQAckConfig.isCallbackControlledAckEnabled()) {
                // Wait for mediation to decide how to acknowledge the message.
                AckDecision ackDecision;
                try {
                    ackDecision = ackDecisionCallback.await(RabbitMQAckConfig.getInboundAckMaxWaitTimeMs());
                } catch (InterruptedException e) {
                    // POOLED THREAD: do NOT re-set interrupt; handle locally to avoid contaminating pool
                    log.warn("Thread interrupted while waiting for ACK decision from mediation."
                            + " Setting to default REQUEUE_ON_ROLLBACK");
                    ackDecision = AckDecision.SET_REQUEUE_ON_ROLLBACK;
                }
                // Fallback if mediation didn't decide in time
                if (ackDecision == null) {
                    log.warn("Timeout while waiting for ACK decision from mediation Setting to"
                            + " default REQUEUE_ON_ROLLBACK");
                    ackDecision = AckDecision.SET_REQUEUE_ON_ROLLBACK;
                }
                setAckDecisionProperty(ackDecision, msgContext);
            }

            return getAcknowledgementMode(msgContext);
        } catch (Throwable fault) {
            log.error("Error when trying to read incoming message.", fault);
            return AcknowledgementMode.REQUEUE_FALSE;
        }
    }

    private void setAckDecisionProperty(AckDecision ackDecision, MessageContext msgContext) {
        String acknowledgementMode = ackDecision.toStringValue();
        msgContext.removeProperty(RabbitMQConstants.ACKNOWLEDGEMENT_DECISION);
        switch (acknowledgementMode) {
            case "ACKNOWLEDGE":
                break;
            case "SET_ROLLBACK_ONLY":
                msgContext.setProperty(BaseConstants.SET_ROLLBACK_ONLY, Boolean.TRUE);
                break;
            case "SET_REQUEUE_ON_ROLLBACK":
                msgContext.setProperty(RabbitMQConstants.SET_REQUEUE_ON_ROLLBACK, Boolean.TRUE);
                break;
            default:
                // do nothing
        }
    }

    private AcknowledgementMode getAcknowledgementMode(MessageContext msgContext) {
        AcknowledgementMode acknowledgementMode;
        if (isBooleanPropertySet(BaseConstants.SET_ROLLBACK_ONLY, msgContext)) {
            acknowledgementMode = AcknowledgementMode.REQUEUE_FALSE;
        } else if (isBooleanPropertySet(RabbitMQConstants.SET_REQUEUE_ON_ROLLBACK, msgContext)) {
            acknowledgementMode = AcknowledgementMode.REQUEUE_TRUE;
        } else {
            acknowledgementMode = AcknowledgementMode.ACKNOWLEDGE;
        }
        return acknowledgementMode;
    }

    private boolean isBooleanPropertySet(String propertyName, MessageContext messageContext) {
        Object property = messageContext.getProperty(propertyName);
        return (property instanceof Boolean && ((Boolean) property)) ||
                (property instanceof String && Boolean.parseBoolean((String) property));
    }
}

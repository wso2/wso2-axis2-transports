/*
* Copyright 2004,2005 The Apache Software Foundation.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.axis2.transport.oracleaq;

import oracle.jms.AdtMessage;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.MetricsCollector;
import org.apache.axis2.transport.oracleaq.ctype.ContentTypeInfo;
import org.apache.axis2.context.MessageContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import javax.transaction.UserTransaction;

import static org.apache.axis2.transport.oracleaq.OracleAQConstants.JMS_MESSAGE_DELIVERY_COUNT_HEADER;

/**
 * This is the JMS message receiver which is invoked when a message is received. This processes
 * the message through the engine
 */
public class OracleAQMessageReceiver {

    private static final Log log = LogFactory.getLog(OracleAQMessageReceiver.class);

    /** The OracleAQListener */
    private OracleAQListener oracleAQListener = null;
    /** A reference to the JMS Connection Factory */
    private OracleAQConnectionFactory jmsConnectionFactory = null;
    /** The JMS metrics collector */
    private MetricsCollector metrics = null;
    /** The endpoint this message receiver is bound to */
    final OracleAQEndpoint endpoint;

    /**
     * Create a new JMSMessage receiver
     *
     * @param oracleAQListener the JMS transport Listener
     * @param jmsConFac   the JMS connection factory we are associated with
     * @param endpoint    the OracleAQEndpoint definition to be used
     */
    OracleAQMessageReceiver(OracleAQListener oracleAQListener, OracleAQConnectionFactory jmsConFac, OracleAQEndpoint endpoint) {
        this.oracleAQListener = oracleAQListener;
        this.jmsConnectionFactory = jmsConFac;
        this.endpoint = endpoint;
        this.metrics = oracleAQListener.getMetricsCollector();
    }

    /**
     * Process a new message received
     *
     * @param message the JMS message received
     * @param ut      UserTransaction which was used to receive the message
     * @return true if caller should commit
     */
    public boolean onMessage(AdtMessage message, UserTransaction ut) {

        try {
            if (log.isDebugEnabled()) {
                StringBuffer sb = new StringBuffer();
                sb.append("Received new JMS message for service :").append(endpoint.getServiceName());
                sb.append("\nDestination    : ").append(message.getJMSDestination());
                sb.append("\nMessage ID     : ").append(message.getJMSMessageID());
                sb.append("\nCorrelation ID : ").append(message.getJMSCorrelationID());
                sb.append("\nReplyTo        : ").append(message.getJMSReplyTo());
                sb.append("\nRedelivery ?   : ").append(message.getJMSRedelivered());
                sb.append("\nPriority       : ").append(message.getJMSPriority());
                sb.append("\nExpiration     : ").append(message.getJMSExpiration());
                sb.append("\nTimestamp      : ").append(message.getJMSTimestamp());
                sb.append("\nMessage Type   : ").append(message.getJMSType());
                sb.append("\nPersistent ?   : ").append(
                    DeliveryMode.PERSISTENT == message.getJMSDeliveryMode());

                log.debug(sb.toString());
                if (log.isTraceEnabled() && message instanceof TextMessage) {
                    log.trace("\nMessage : " + ((TextMessage) message).getText());
                }
            }
        } catch (JMSException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error reading JMS message headers for debug logging", e);
            }
        }

        // update transport level metrics
        try {
            metrics.incrementBytesReceived(OracleAQUtils.getMessageSize(message));
        } catch (JMSException e) {
            log.warn("Error reading JMS message size to update transport metrics", e);
        }

        // has this message already expired? expiration time == 0 means never expires
        // TODO: explain why this is necessary; normally it is the responsibility of the provider to handle message expiration
        try {
            long expiryTime = message.getJMSExpiration();
            if (expiryTime > 0 && System.currentTimeMillis() > expiryTime) {
                if (log.isDebugEnabled()) {
                    log.debug("Discard expired message with ID : " + message.getJMSMessageID());
                }
                return true;
            }
        } catch (JMSException ignore) {}


        boolean successful = false;
        try {
            successful = processThoughEngine(message, ut);

        } catch (JMSException e) {
            log.error("JMS Exception encountered while processing", e);
        } catch (AxisFault e) {
            log.error("Axis fault processing message", e);
        } catch (Exception e) {
            log.error("Unknown error processing message", e);

        } finally {
            if (successful) {
                metrics.incrementMessagesReceived();
            } else {
                metrics.incrementFaultsReceiving();
            }
        }

        return successful;
    }

    /**
     * Process the new message through Axis2
     *
     * @param message the JMS message
     * @param ut      the UserTransaction used for receipt
     * @return true if the caller should commit
     * @throws JMSException, on JMS exceptions
     * @throws AxisFault     on Axis2 errors
     */
    private boolean processThoughEngine(AdtMessage message, UserTransaction ut)
        throws JMSException, AxisFault {

        MessageContext msgContext = endpoint.createMessageContext();

        // set the JMS Message ID as the Message ID of the MessageContext
        try {
            msgContext.setMessageID(message.getJMSMessageID());
            String jmsCorrelationID = message.getJMSCorrelationID();
            if (jmsCorrelationID != null && jmsCorrelationID.length() > 0) {
                msgContext.setProperty(OracleAQConstants.JMS_COORELATION_ID, jmsCorrelationID);
            } else {
                msgContext.setProperty(OracleAQConstants.JMS_COORELATION_ID, message.getJMSMessageID());
            }
        } catch (JMSException ignore) {}

        String soapAction = OracleAQUtils.getProperty(message, BaseConstants.SOAPACTION);

        ContentTypeInfo contentTypeInfo =
            endpoint.getContentTypeRuleSet().getContentTypeInfo(message);
        if (contentTypeInfo == null) {
            throw new AxisFault("Unable to determine content type for message " +
                msgContext.getMessageID());
        }

        // set the message property OUT_TRANSPORT_INFO
        // the reply is assumed to be over the JMSReplyTo destination, using
        // the same incoming connection factory, if a JMSReplyTo is available
        /*Destination replyTo = message.getJMSReplyTo();
        if (replyTo == null) {
            // does the service specify a default reply destination ?
            String jndiReplyDestinationName = endpoint.getJndiReplyDestinationName();
            if (jndiReplyDestinationName != null) {
                replyTo = jmsConnectionFactory.getDestination(jndiReplyDestinationName,
                        endpoint.getReplyDestinationType());
            }

        }
        if (replyTo != null) {
            msgContext.setProperty(Constants.OUT_TRANSPORT_INFO,
                new OracleAQOutTransportInfo(jmsConnectionFactory, replyTo,
                    contentTypeInfo.getPropertyName()));
        }*/

        // Setting JMSXDeliveryCount header on the message context
        try {
            int deliveryCount = message.getIntProperty(JMS_MESSAGE_DELIVERY_COUNT_HEADER);
            msgContext.setProperty(OracleAQConstants.DELIVERY_COUNT, deliveryCount);
        } catch (NumberFormatException nfe) {
            if (log.isDebugEnabled()) {
                log.debug("JMSXDeliveryCount is not set in the received message");
            }
        }

        OracleAQUtils.setSOAPEnvelope(message, msgContext, contentTypeInfo.getContentType());
        if (ut != null) {
            msgContext.setProperty(BaseConstants.USER_TRANSACTION, ut);
        }

        msgContext.setProperty(OracleAQConstants.PARAM_JMS_HYPHEN_MODE, endpoint.getHyphenSupport());

        oracleAQListener.handleIncomingMessage(
                msgContext,
                OracleAQUtils.getTransportHeaders(message, msgContext),
                soapAction,
                contentTypeInfo.getContentType());

        Object o = msgContext.getProperty(BaseConstants.SET_ROLLBACK_ONLY);
        if (o != null) {
            if ((o instanceof Boolean && ((Boolean) o)) ||
                    (o instanceof String && Boolean.valueOf((String) o))) {
                return false;
            }
        }
        return true;
    }

    protected OracleAQListener getOracleAQListener() {
        return oracleAQListener;
    }

}

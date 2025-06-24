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
package org.apache.axis2.transport.jms;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMNode;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.om.OMText;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.base.AbstractTransportSender;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.transport.base.ManagementSupport;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.axis2.transport.jms.iowrappers.BytesMessageOutputStream;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.commons.io.output.WriterOutputStream;
import org.wso2.securevault.SecretResolver;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.activation.DataHandler;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * The TransportSender for JMS
 */
public class JMSSender extends AbstractTransportSender implements ManagementSupport {

    public static final String TRANSPORT_NAME = Constants.TRANSPORT_JMS;

    private static Map<Transaction, ArrayList<JMSMessageSender>> jmsMessageSenderMap = new HashMap<>();

    private static Map<Transaction, ArrayList<org.apache.axis2.transport.jms.jakarta.JMSMessageSender>>
            jakartaMessageSenderMap = new HashMap<>();
    /** The JMS connection factory manager to be used when sending messages out */
    private JMSConnectionFactoryManager connFacManager;

    /**
     * Initialize the transport sender by reading pre-defined connection factories for
     * outgoing messages.
     *
     * @param cfgCtx the configuration context
     * @param transportOut the transport sender definition from axis2.xml
     * @throws AxisFault on error
     */
    @Override
    public void init(ConfigurationContext cfgCtx, TransportOutDescription transportOut) throws AxisFault {
        super.init(cfgCtx, transportOut);
        SecretResolver secretResolver = cfgCtx.getAxisConfiguration().getSecretResolver();
        connFacManager = new JMSConnectionFactoryManager(transportOut, secretResolver);
        log.info("JMS Transport Sender initialized...");
    }
    
    @Override
    public void stop() {
        
        // clean up any shared JMS resources in this sender's connection factories
        connFacManager.stop();
        
        super.stop();
    }

    /**
     * Get corresponding JMS connection factory defined within the transport sender for the
     * transport-out information - usually constructed from a targetEPR
     *
     * @param trpInfo the transport-out information
     * @return the corresponding JMS connection factory, if any
     */
    private JMSConnectionFactory getJMSConnectionFactory(JMSOutTransportInfo trpInfo) {
        Map<String,String> props = trpInfo.getProperties();
        if (trpInfo.getProperties() != null) {
            String jmsConnectionFactoryName = props.get(JMSConstants.PARAM_JMS_CONFAC);
			
            log.debug("Using Connection factory:" + jmsConnectionFactoryName);
            if (jmsConnectionFactoryName != null) {
            	log.debug(JMSConstants.PARAM_JMS_CONFAC + " is used");

                return connFacManager.getJMSConnectionFactory(jmsConnectionFactoryName);
            } else {
                JMSConnectionFactory fac = connFacManager.getJMSConnectionFactory(props);
                if (fac == null) {
                    fac = connFacManager.getJMSConnectionFactory(JMSConstants.DEFAULT_CONFAC_NAME);
		    log.debug("Connection factory has been initialized");
                } else {
                	log.debug("Connection factory has been re-used");
                }
                return  fac;
            }
        } else {
            return null;
        }
    }

    /**
     * Get corresponding JMS connection factory defined within the transport sender for the
     * transport-out information - usually constructed from a targetEPR
     *
     * @param trpInfo the transport-out information
     * @return the corresponding JMS connection factory, if any
     */
    private org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory getJakartaConnectionFactory(
            org.apache.axis2.transport.jms.jakarta.JMSOutTransportInfo trpInfo) {
        Map<String,String> props = trpInfo.getProperties();
        if (trpInfo.getProperties() != null) {
            String jmsConnectionFactoryName = props.get(JMSConstants.PARAM_JMS_CONFAC);

            log.debug("Using Connection factory:" + jmsConnectionFactoryName);
            if (jmsConnectionFactoryName != null) {
                log.debug(JMSConstants.PARAM_JMS_CONFAC + " is used");

                return connFacManager.getJakartaConnectionFactory(jmsConnectionFactoryName);
            } else {
                org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory fac =
                        connFacManager.getJakartaConnectionFactory(props);
                if (fac == null) {
                    fac = connFacManager.getJakartaConnectionFactory(JMSConstants.DEFAULT_CONFAC_NAME);
                    log.debug("Connection factory has been initialized");
                } else {
                    log.debug("Connection factory has been re-used");
                }
                return  fac;
            }
        } else {
            return null;
        }
    }

    /**
     * Performs the actual sending of the JMS message
     */
    @Override
    public void sendMessage(MessageContext msgCtx, String targetAddress,
        OutTransportInfo outTransportInfo) throws AxisFault {

        JMSConnectionFactory jmsConnectionFactory = null;
        JMSOutTransportInfo jmsOut = null;
        JMSMessageSender messageSender = null;

        org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory jakartaConnectionFactory = null;
        org.apache.axis2.transport.jms.jakarta.JMSOutTransportInfo jakartaOut = null;
        org.apache.axis2.transport.jms.jakarta.JMSMessageSender jakartaMessageSender = null;

        boolean isJmsSpec31 = false;

        if (targetAddress != null) {

            Hashtable<String, String> parameters = BaseUtils.getEPRProperties(targetAddress);
            isJmsSpec31 = JMSUtils.isJmsSpec31(parameters);

            if (isJmsSpec31) {

                jakartaOut = new org.apache.axis2.transport.jms.jakarta.JMSOutTransportInfo(targetAddress);
                // do we have a definition for a connection factory to use for this address?
                jakartaConnectionFactory = getJakartaConnectionFactory(jakartaOut);

                if (jakartaConnectionFactory == null) {
                    jakartaConnectionFactory = connFacManager.getJakartaConnectionFactoryFromTargetEndpoint(targetAddress);
                }
                // MessageSender only needed to add if the scenario involved with XA transaction.
                // It can be identified by looking at target url's transport.jms.TransactionCommand parameter.
                if ((targetAddress.contains(JMSConstants.JMS_TRANSACTION_COMMAND))) {
                    try {
                        jakartaMessageSender = jakartaOut.createJMSSender(msgCtx);
                        Transaction transaction = (Transaction) msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION);
                        if (jakartaMessageSenderMap.get(transaction) == null) {
                            ArrayList list = new ArrayList();
                            list.add(jakartaMessageSender);
                            jakartaMessageSenderMap.put(transaction, list);
                        } else {
                            ArrayList list = jakartaMessageSenderMap.get(transaction);
                            list.add(jakartaMessageSender);
                            jakartaMessageSenderMap.put(transaction, list);
                        }
                    } catch (jakarta.jms.JMSException e) {
                        rollbackIfXATransaction(msgCtx, true);
                        handleException("Unable to create a JMSMessageSender for : " + outTransportInfo, e);
                    }
                } else {
                    jakartaMessageSender = new org.apache.axis2.transport.jms.jakarta.JMSMessageSender(
                            jakartaConnectionFactory, targetAddress);
                }

            } else {

                jmsOut = new JMSOutTransportInfo(targetAddress);
                // do we have a definition for a connection factory to use for this address?
                jmsConnectionFactory = getJMSConnectionFactory(jmsOut);

                if (jmsConnectionFactory == null) {
                    jmsConnectionFactory = connFacManager.getConnectionFactoryFromTargetEndpoint(targetAddress);
                }
                // MessageSender only needed to add if the scenario involved with XA transaction.
                // It can be identified by looking at target url's transport.jms.TransactionCommand parameter.
                if ((targetAddress.contains(JMSConstants.JMS_TRANSACTION_COMMAND))) {
                    try {
                        messageSender = jmsOut.createJMSSender(msgCtx);
                        Transaction transaction = (Transaction) msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION);
                        if (jmsMessageSenderMap.get(transaction) == null) {
                            ArrayList list = new ArrayList();
                            list.add(messageSender);
                            jmsMessageSenderMap.put(transaction, list);
                        } else {
                            ArrayList list = jmsMessageSenderMap.get(transaction);
                            list.add(messageSender);
                            jmsMessageSenderMap.put(transaction, list);
                        }
                    } catch (JMSException e) {
                        rollbackIfXATransaction(msgCtx, false);
                        handleException("Unable to create a JMSMessageSender for : " + outTransportInfo, e);
                    }
                } else {
                    messageSender = new JMSMessageSender(jmsConnectionFactory, targetAddress);
                }
            }

        } else if (outTransportInfo instanceof JMSOutTransportInfo) {

            jmsOut = (JMSOutTransportInfo) outTransportInfo;
            try {
                messageSender = jmsOut.createJMSSender(msgCtx);
            } catch (JMSException e) {
                handleException("Unable to create a JMSMessageSender for : " + outTransportInfo, e);
            }
        } else if (outTransportInfo instanceof org.apache.axis2.transport.jms.jakarta.JMSOutTransportInfo) {

            isJmsSpec31 = true;
            jakartaOut = (org.apache.axis2.transport.jms.jakarta.JMSOutTransportInfo) outTransportInfo;
            try {
                jakartaMessageSender = jakartaOut.createJMSSender(msgCtx);
            } catch (jakarta.jms.JMSException e) {
                handleException("Unable to create a JMSMessageSender for : " + outTransportInfo, e);
            }
        }

        // The message property to be used to send the content type is determined by
        // the out transport info, i.e. either from the EPR if we are sending a request,
        // or, if we are sending a response, from the configuration of the service that
        // received the request). The property can be defined globally in axis2.xml JMSSender section as well. 
        // The property name can be overridden by a message context property.
        String contentTypeProperty;
        if (isJmsSpec31) {
            contentTypeProperty = getContentTypePropertyForJakarta(msgCtx, jakartaOut, jakartaConnectionFactory);
            // Fix for ESBJAVA-3687, retrieve JMS transport property transport.jms.MessagePropertyHyphens and set this
            // into the msgCtx
            String hyphenSupport = JMSConstants.DEFAULT_HYPHEN_SUPPORT;
            if (jakartaConnectionFactory != null && jakartaConnectionFactory.getParameters().get(JMSConstants.PARAM_JMS_HYPHEN_MODE) != null) {
                hyphenSupport = jakartaConnectionFactory.getParameters().get(JMSConstants.PARAM_JMS_HYPHEN_MODE);
            } else if (jakartaOut.getProperties() != null && jakartaOut.getProperties().get(JMSConstants.PARAM_JMS_HYPHEN_MODE) != null) {
                if (jakartaOut.getProperties().get(JMSConstants.PARAM_JMS_HYPHEN_MODE).equals(JMSConstants.HYPHEN_MODE_REPLACE)) {
                    hyphenSupport = JMSConstants.HYPHEN_MODE_REPLACE;
                } else if (jakartaOut.getProperties().get(JMSConstants.PARAM_JMS_HYPHEN_MODE).equals(JMSConstants.HYPHEN_MODE_DELETE)) {
                    hyphenSupport = JMSConstants.HYPHEN_MODE_DELETE;
                }
            }
            msgCtx.setProperty(JMSConstants.PARAM_JMS_HYPHEN_MODE, hyphenSupport);

            org.apache.axis2.transport.jms.jakarta.JMSReplyMessage jakartaReplyMessage = null;

            // Set the status of the SessionWrapper as busy
            if (jakartaMessageSender.getSessionWrapper() != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Set the status of the session as busy for " + messageSender.getSessionWrapper().getSession());
                }
                jakartaMessageSender.getSessionWrapper().setBusy(true);
            }

            // need to synchronize as Sessions are not thread safe
            synchronized (jakartaMessageSender.getSession()) {
                try {
                    jakartaReplyMessage = sendOverJakartaJMS(msgCtx, jakartaMessageSender, contentTypeProperty, jakartaConnectionFactory, jakartaOut);
                } finally {
                    if (msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION_MANAGER) == null) {
                        jakartaMessageSender.close();
                        if (jakartaReplyMessage != null && jakartaReplyMessage.getReplyDestination() instanceof jakarta.jms.TemporaryQueue) {
                            String temporaryQueueName = "";
                            jakarta.jms.TemporaryQueue temporaryQueue = (jakarta.jms.TemporaryQueue) jakartaReplyMessage.getReplyDestination();
                            try {
                                temporaryQueueName = temporaryQueue.getQueueName();
                                temporaryQueue.delete();
                            } catch (jakarta.jms.JMSException e) {
                                log.error("Error while deleting the temporary queue " + temporaryQueueName, e);
                            }
                        }
                    }
                    // Set the status of the SessionWrapper as free
                    if (jakartaMessageSender.getSessionWrapper() != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Set the status of the session as free for " + jakartaMessageSender.getSessionWrapper().getSession());
                        }
                        jakartaMessageSender.getSessionWrapper().setBusy(false);
                    }
                }
            }
            // Getting the response flow out of the synchronized block so that the session is released
            // for other threads.
            if (jakartaReplyMessage.getMsgctx() != null) {
                handleIncomingMessage(jakartaReplyMessage.getMsgctx(), jakartaReplyMessage.getTransportHeaders(),
                        jakartaReplyMessage.getSoapAction(), jakartaReplyMessage.getContentType());
            }
        } else {
            contentTypeProperty = getContentTypePropertyForJavax(msgCtx, jmsOut, jmsConnectionFactory);
            // Fix for ESBJAVA-3687, retrieve JMS transport property transport.jms.MessagePropertyHyphens and set this
            // into the msgCtx
            String hyphenSupport = JMSConstants.DEFAULT_HYPHEN_SUPPORT;
            if (jmsConnectionFactory != null && jmsConnectionFactory.getParameters().get(JMSConstants.PARAM_JMS_HYPHEN_MODE) != null) {
                hyphenSupport = jmsConnectionFactory.getParameters().get(JMSConstants.PARAM_JMS_HYPHEN_MODE);
            } else if (jmsOut.getProperties() != null && jmsOut.getProperties().get(JMSConstants.PARAM_JMS_HYPHEN_MODE) != null) {
                if (jmsOut.getProperties().get(JMSConstants.PARAM_JMS_HYPHEN_MODE).equals(JMSConstants.HYPHEN_MODE_REPLACE)) {
                    hyphenSupport = JMSConstants.HYPHEN_MODE_REPLACE;
                } else if (jmsOut.getProperties().get(JMSConstants.PARAM_JMS_HYPHEN_MODE).equals(JMSConstants.HYPHEN_MODE_DELETE)) {
                    hyphenSupport = JMSConstants.HYPHEN_MODE_DELETE;
                }
            }
            msgCtx.setProperty(JMSConstants.PARAM_JMS_HYPHEN_MODE, hyphenSupport);

            JMSReplyMessage jmsReplyMessage = null;

            // Set the status of the SessionWrapper as busy
            if (messageSender.getSessionWrapper() != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Set the status of the session as busy for " + messageSender.getSessionWrapper().getSession());
                }
                messageSender.getSessionWrapper().setBusy(true);
            }

            // need to synchronize as Sessions are not thread safe
            synchronized (messageSender.getSession()) {
                try {
                    jmsReplyMessage = sendOverJMS(msgCtx, messageSender, contentTypeProperty, jmsConnectionFactory, jmsOut);
                } finally {
                    if (msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION_MANAGER) == null) {
                        messageSender.close();
                        if (jmsReplyMessage != null && jmsReplyMessage.getReplyDestination() instanceof TemporaryQueue) {
                            String temporaryQueueName = "";
                            TemporaryQueue temporaryQueue = (TemporaryQueue) jmsReplyMessage.getReplyDestination();
                            try {
                                temporaryQueueName = temporaryQueue.getQueueName();
                                temporaryQueue.delete();
                            } catch (JMSException e) {
                                log.error("Error while deleting the temporary queue " + temporaryQueueName, e);
                            }
                        }
                    }
                    // Set the status of the SessionWrapper as free
                    if (messageSender.getSessionWrapper() != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Set the status of the session as free for " + messageSender.getSessionWrapper().getSession());
                        }
                        messageSender.getSessionWrapper().setBusy(false);
                    }
                }
            }
            // Getting the response flow out of the synchronized block so that the session is released
            // for other threads.
            if (jmsReplyMessage.getMsgctx() != null) {
                handleIncomingMessage(jmsReplyMessage.getMsgctx(), jmsReplyMessage.getTransportHeaders(),
                        jmsReplyMessage.getSoapAction(), jmsReplyMessage.getContentType());
            }
        }
    }

    private void sendJavaxMessageFromTargetAddress() {

    }

    /**
     * Trying to rollback if type is XA transaction.
     * @param msgCtx MessageContext.
     * @throws AxisFault
     */
    private void rollbackIfXATransaction(MessageContext msgCtx, boolean isJmsSpec31) throws AxisFault {
        Transaction transaction = null;
        try {
            if (msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION_MANAGER) != null) {
                transaction = ((TransactionManager) msgCtx.getProperty(JMSConstants
                        .JMS_XA_TRANSACTION_MANAGER)).getTransaction();
                if (isJmsSpec31) {
                    rollbackJakartaXATransaction(transaction);
                } else {
                    rollbackJavaxXATransaction(transaction);
                }
            }
        } catch (SystemException e1) {
            handleException("Error occurred during obtaining  transaction", e1);
        }
    }

    /**
     * Retrieves the contentType property looking at the message context, jms outbound transport information 
     * and connection factory parameters
     * 
     * @param msgCtx current message context
     * @param jmsOut JMS outbound transport information 
     * @param jmsConnectionFactory JMS connection factory
     * @return the content type property name
     */
    protected String getContentTypePropertyForJavax(MessageContext msgCtx, JMSOutTransportInfo jmsOut,
                                                    JMSConnectionFactory jmsConnectionFactory) {

        String contentTypeProperty =
                (String) msgCtx.getProperty(JMSConstants.CONTENT_TYPE_PROPERTY_PARAM);
        if (contentTypeProperty == null) {
            contentTypeProperty = jmsOut.getContentTypeProperty();
        }
        if (contentTypeProperty == null && jmsConnectionFactory != null) {
            contentTypeProperty = jmsConnectionFactory.getParameters().get(JMSConstants.CONTENT_TYPE_PROPERTY_PARAM);
        }
        return contentTypeProperty;
    }

    /**
     * Retrieves the contentType property looking at the message context, jms outbound transport information
     * and connection factory parameters
     *
     * @param msgCtx current message context
     * @param jmsOut JMS outbound transport information
     * @param jmsConnectionFactory JMS connection factory
     * @return the content type property name
     */
    protected String getContentTypePropertyForJakarta(
            MessageContext msgCtx,
            org.apache.axis2.transport.jms.jakarta.JMSOutTransportInfo jmsOut,
            org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory jmsConnectionFactory) {

        String contentTypeProperty =
                (String) msgCtx.getProperty(JMSConstants.CONTENT_TYPE_PROPERTY_PARAM);
        if (contentTypeProperty == null) {
            contentTypeProperty = jmsOut.getContentTypeProperty();
        }
        if (contentTypeProperty == null && jmsConnectionFactory != null) {
            contentTypeProperty = jmsConnectionFactory.getParameters().get(JMSConstants.CONTENT_TYPE_PROPERTY_PARAM);
        }
        return contentTypeProperty;
    }

    /**
     * Perform actual sending of the JMS message
     */
    private JMSReplyMessage sendOverJMS(MessageContext msgCtx, JMSMessageSender messageSender,
        String contentTypeProperty, JMSConnectionFactory jmsConnectionFactory,
        JMSOutTransportInfo jmsOut) throws AxisFault {

        JMSReplyMessage jmsReplyMessage = null;
        // convert the axis message context into a JMS Message that we can send over JMS
        Message message = null;
        String correlationId = null;
        try {
            message = createJMSMessage(msgCtx, messageSender.getSession(), contentTypeProperty);
        } catch (JMSException e) {
            rollbackIfXATransaction(msgCtx, false);
            jmsConnectionFactory.clearCache(messageSender.getConnection());
            handleException("Error creating a JMS message from the message context", e);
        }

        // should we wait for a synchronous response on this same thread?
        boolean waitForResponse = waitForSynchronousResponse(msgCtx);
        Destination replyDestination = jmsOut.getReplyDestination();

        // if this is a synchronous out-in, prepare to listen on the response destination
        String replyDestName;

        if (isWaitForResponseOrReplyDestination(waitForResponse, replyDestination, msgCtx)) { //check replyDestination for APIMANAGER-5892

            replyDestName = (String) msgCtx.getProperty(JMSConstants.JMS_REPLY_TO);
            if (replyDestName == null && jmsConnectionFactory != null) {
                if (jmsOut != null && jmsOut.getReplyDestinationName() != null) {
                    replyDestName = jmsOut.getReplyDestinationName();
                } else {
                    replyDestName = jmsConnectionFactory.getReplyToDestination();
                }
            }

            String replyDestType = (String) msgCtx.getProperty(JMSConstants.JMS_REPLY_TO_TYPE);
            if (replyDestType == null && jmsConnectionFactory != null) {
                replyDestType = jmsConnectionFactory.getReplyDestinationType();
            }

            if (replyDestName != null) {
                if (jmsConnectionFactory != null) {
                    replyDestination = jmsConnectionFactory.getDestination(
                            replyDestName, replyDestType);
                } else {
                    replyDestination = jmsOut.getReplyDestination(replyDestName);
                }
            }
            replyDestination = JMSUtils.setReplyDestination(
                replyDestination, messageSender.getSession(), message);

        }

        try {
            if (msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION) != null) {
                messageSender.send(message, msgCtx);
                Transaction transaction = (Transaction) msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION);
                if (msgCtx.getTo().toString().contains("transport.jms.TransactionCommand=end")) {
                    commitXAJavaxTransaction(transaction);
                } else if (msgCtx.getTo().toString().contains("transport.jms.TransactionCommand=rollback")) {
                    rollbackJavaxXATransaction(transaction);
                }
            } else {
                messageSender.transactionallySend(message, msgCtx);
            }

            metrics.incrementMessagesSent(msgCtx);

        } catch (AxisJMSException e) {
            metrics.incrementFaultsSending();
            rollbackIfXATransaction(msgCtx, false);
            jmsConnectionFactory.clearCache(messageSender.getConnection());
            handleException("Error sending JMS message", e);
        } catch (JMSException e) {
            handleException("Error sending JMS message to destination", e);
        }

        try {
            metrics.incrementBytesSent(msgCtx, JMSUtils.getMessageSize(message));
        } catch (JMSException e) {
            log.warn("Error reading JMS message size to update transport metrics", e);
        }

        // if we are expecting a synchronous response back for the message sent out
        if (isWaitForResponseOrReplyDestination(waitForResponse, replyDestination, msgCtx)) {
            // TODO ********************************************************************************
            // TODO **** replace with asynchronous polling via a poller task to process this *******
            // information would be given. Then it should poll (until timeout) the
            // requested destination for the response message and inject it from a
            // asynchronous worker thread
            try {
                String jmsCorrelationID = message.getJMSCorrelationID();
                if (jmsCorrelationID != null && jmsCorrelationID.length() > 0) {
                    correlationId = jmsCorrelationID;
                } else {
                    correlationId = message.getJMSMessageID();
                }
            } catch(JMSException ignore) {}

            // We assume here that the response uses the same message property to
            // specify the content type of the message.
            jmsReplyMessage = waitForResponseAndProcess(messageSender.getSession(), replyDestination,
                msgCtx, correlationId, contentTypeProperty);
            // TODO ********************************************************************************
        }
        if (jmsReplyMessage == null) {
            jmsReplyMessage = new JMSReplyMessage();
        }
        jmsReplyMessage.setReplyDestination(replyDestination);
        return jmsReplyMessage;
    }

    /**
     * Perform actual sending of the JMS message
     */
    private org.apache.axis2.transport.jms.jakarta.JMSReplyMessage sendOverJakartaJMS(MessageContext msgCtx,
                                               org.apache.axis2.transport.jms.jakarta.JMSMessageSender messageSender,
                                               String contentTypeProperty,
                                               org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory jmsConnectionFactory,
                                               org.apache.axis2.transport.jms.jakarta.JMSOutTransportInfo jmsOut) throws AxisFault {

        org.apache.axis2.transport.jms.jakarta.JMSReplyMessage jmsReplyMessage = null;
        // convert the axis message context into a JMS Message that we can send over JMS
        jakarta.jms.Message message = null;
        String correlationId = null;
        try {
            message = createJMSJakartaMessage(msgCtx, messageSender.getSession(), contentTypeProperty);
        } catch (jakarta.jms.JMSException e) {
            rollbackIfXATransaction(msgCtx, true);
            jmsConnectionFactory.clearCache(messageSender.getConnection());
            handleException("Error creating a JMS message from the message context", e);
        }

        // should we wait for a synchronous response on this same thread?
        boolean waitForResponse = waitForSynchronousResponse(msgCtx);
        jakarta.jms.Destination replyDestination = jmsOut.getReplyDestination();

        // if this is a synchronous out-in, prepare to listen on the response destination
        String replyDestName;

        if (isWaitForResponseOrJakartaReplyDestination(waitForResponse, replyDestination, msgCtx)) { //check replyDestination for APIMANAGER-5892

            replyDestName = (String) msgCtx.getProperty(JMSConstants.JMS_REPLY_TO);
            if (replyDestName == null && jmsConnectionFactory != null) {
                if (jmsOut != null && jmsOut.getReplyDestinationName() != null) {
                    replyDestName = jmsOut.getReplyDestinationName();
                } else {
                    replyDestName = jmsConnectionFactory.getReplyToDestination();
                }
            }

            String replyDestType = (String) msgCtx.getProperty(JMSConstants.JMS_REPLY_TO_TYPE);
            if (replyDestType == null && jmsConnectionFactory != null) {
                replyDestType = jmsConnectionFactory.getReplyDestinationType();
            }

            if (replyDestName != null) {
                if (jmsConnectionFactory != null) {
                    replyDestination = jmsConnectionFactory.getDestination(
                            replyDestName, replyDestType);
                } else {
                    replyDestination = jmsOut.getReplyDestination(replyDestName);
                }
            }
            replyDestination = org.apache.axis2.transport.jms.jakarta.JMSUtils.setReplyDestination(
                    replyDestination, messageSender.getSession(), message);

        }

        try {
            if (msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION) != null) {
                messageSender.send(message, msgCtx);
                Transaction transaction = (Transaction) msgCtx.getProperty(JMSConstants.JMS_XA_TRANSACTION);
                if (msgCtx.getTo().toString().contains("transport.jms.TransactionCommand=end")) {
                    commitXAJakartaTransaction(transaction);
                } else if (msgCtx.getTo().toString().contains("transport.jms.TransactionCommand=rollback")) {
                    rollbackJakartaXATransaction(transaction);
                }
            } else {
                messageSender.transactionallySend(message, msgCtx);
            }

            metrics.incrementMessagesSent(msgCtx);

        } catch (AxisJMSException e) {
            metrics.incrementFaultsSending();
            rollbackIfXATransaction(msgCtx, true);
            jmsConnectionFactory.clearCache(messageSender.getConnection());
            handleException("Error sending JMS message", e);
        } catch (jakarta.jms.JMSException e) {
            handleException("Error sending JMS message to destination", e);
        }

        try {
            metrics.incrementBytesSent(msgCtx, org.apache.axis2.transport.jms.jakarta.JMSUtils.getMessageSize(message));
        } catch (jakarta.jms.JMSException e) {
            log.warn("Error reading JMS message size to update transport metrics", e);
        }

        // if we are expecting a synchronous response back for the message sent out
        if (isWaitForResponseOrJakartaReplyDestination(waitForResponse, replyDestination, msgCtx)) {
            // TODO ********************************************************************************
            // TODO **** replace with asynchronous polling via a poller task to process this *******
            // information would be given. Then it should poll (until timeout) the
            // requested destination for the response message and inject it from a
            // asynchronous worker thread
            try {
                String jmsCorrelationID = message.getJMSCorrelationID();
                if (jmsCorrelationID != null && jmsCorrelationID.length() > 0) {
                    correlationId = jmsCorrelationID;
                } else {
                    correlationId = message.getJMSMessageID();
                }
            } catch(jakarta.jms.JMSException ignore) {}

            // We assume here that the response uses the same message property to
            // specify the content type of the message.
            jmsReplyMessage = waitForResponseAndProcessForJakarta(messageSender.getSession(), replyDestination,
                    msgCtx, correlationId, contentTypeProperty);
            // TODO ********************************************************************************
        }
        if (jmsReplyMessage == null) {
            jmsReplyMessage = new org.apache.axis2.transport.jms.jakarta.JMSReplyMessage();
        }
        jmsReplyMessage.setReplyDestination(replyDestination);
        return jmsReplyMessage;
    }

    private boolean hasDestinationEPR(MessageContext msgContext) {
        String transportURL = (String) msgContext.getProperty(
                Constants.Configuration.TRANSPORT_URL);

        if (transportURL != null) {
            return true;
        } else {
            return (msgContext.getTo() != null) && !msgContext.getTo().hasAnonymousAddress();
        }
    }

    protected boolean isWaitForResponseOrReplyDestination(boolean waitForResponse, Destination replyDestination) {
        return waitForResponse || replyDestination != null;
    }

    protected boolean isWaitForResponseOrJakartaReplyDestination(boolean waitForResponse, jakarta.jms.Destination replyDestination) {
        return waitForResponse || replyDestination != null;
    }

    protected boolean isWaitForResponseOrReplyDestination(boolean waitForResponse, Destination replyDestination,
            MessageContext messageContext) {

        return isWaitForResponseOrReplyDestination(waitForResponse, replyDestination) &&
                hasDestinationEPR(messageContext);
    }

    protected boolean isWaitForResponseOrJakartaReplyDestination(boolean waitForResponse, jakarta.jms.Destination replyDestination,
                                                          MessageContext messageContext) {

        return isWaitForResponseOrJakartaReplyDestination(waitForResponse, replyDestination) &&
                hasDestinationEPR(messageContext);
    }

    /**
     * Create a Consumer for the reply destination and wait for the response JMS message
     * synchronously. If a message arrives within the specified time interval, process it
     * through Axis2
     *
     * @param session             the session to use to listen for the response
     * @param replyDestination    the JMS reply Destination
     * @param msgCtx              the outgoing message for which we are expecting the response
     * @param contentTypeProperty the message property used to determine the content type
     *                            of the response message
     * @return Bean containing response message information.
     * @throws AxisFault on error
     */
    private JMSReplyMessage waitForResponseAndProcess(Session session, Destination replyDestination,
                                                      MessageContext msgCtx, String correlationId,
                                                      String contentTypeProperty) throws AxisFault {
        JMSReplyMessage jmsReplyMessage = null;
        MessageConsumer consumer = null;
        try {
            consumer = JMSUtils.createConsumer(session, replyDestination,
                "JMSCorrelationID = '" + correlationId + "'");

            // how long are we willing to wait for the sync response
            long timeout = JMSConstants.DEFAULT_JMS_TIMEOUT;
            String waitReply = (String) msgCtx.getProperty(JMSConstants.JMS_WAIT_REPLY);
            if (waitReply != null) {
                timeout = Long.valueOf(waitReply).longValue();
            }

            if (log.isDebugEnabled()) {
                log.debug("Waiting for a maximum of " + timeout +
                    "ms for a response message to destination : " + replyDestination +
                    " with JMS correlation ID : " + correlationId);
            }

            Message reply = consumer.receive(timeout);

            if (reply != null) {

                // update transport level metrics
                metrics.incrementMessagesReceived();                
                try {
                    metrics.incrementBytesReceived(JMSUtils.getMessageSize(reply));
                } catch (JMSException e) {
                    log.warn("Error reading JMS message size to update transport metrics", e);
                }

                try {
                    jmsReplyMessage = processSyncResponse(msgCtx, reply, contentTypeProperty);
                    metrics.incrementMessagesReceived();
                } catch (AxisFault e) {
                    metrics.incrementFaultsReceiving();
                    throw e;
                }

            } else {
            	                   
            	metrics.incrementFaultsReceiving();
            	handleException("Did not receive a JMS response within " + timeout +
            	                " ms to destination : " + replyDestination +
            	                " with JMS correlation ID : " + correlationId);
            }

        } catch (JMSException e) {
            metrics.incrementFaultsReceiving();
            handleException("Error creating a consumer, or receiving a synchronous reply " +
                "for outgoing MessageContext ID : " + msgCtx.getMessageID() +
                " and reply Destination : " + replyDestination, e);
        } finally {
            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (JMSException e) {
                handleException(
                        "Unable to close consumer for reply destination: " + replyDestination, e);
            }
        }
        return jmsReplyMessage;
    }

    /**
     * Create a Consumer for the reply destination and wait for the response JMS message
     * synchronously. If a message arrives within the specified time interval, process it
     * through Axis2
     *
     * @param session             the session to use to listen for the response
     * @param replyDestination    the JMS reply Destination
     * @param msgCtx              the outgoing message for which we are expecting the response
     * @param contentTypeProperty the message property used to determine the content type
     *                            of the response message
     * @return Bean containing response message information.
     * @throws AxisFault on error
     */
    private org.apache.axis2.transport.jms.jakarta.JMSReplyMessage waitForResponseAndProcessForJakarta(jakarta.jms.Session session,
                                                                jakarta.jms.Destination replyDestination,
                                                      MessageContext msgCtx, String correlationId,
                                                      String contentTypeProperty) throws AxisFault {
        org.apache.axis2.transport.jms.jakarta.JMSReplyMessage jmsReplyMessage = null;
        jakarta.jms.MessageConsumer consumer = null;
        try {
            consumer = org.apache.axis2.transport.jms.jakarta.JMSUtils.createConsumer(session, replyDestination,
                    "JMSCorrelationID = '" + correlationId + "'");

            // how long are we willing to wait for the sync response
            long timeout = JMSConstants.DEFAULT_JMS_TIMEOUT;
            String waitReply = (String) msgCtx.getProperty(JMSConstants.JMS_WAIT_REPLY);
            if (waitReply != null) {
                timeout = Long.valueOf(waitReply).longValue();
            }

            if (log.isDebugEnabled()) {
                log.debug("Waiting for a maximum of " + timeout +
                        "ms for a response message to destination : " + replyDestination +
                        " with JMS correlation ID : " + correlationId);
            }

            jakarta.jms.Message reply = consumer.receive(timeout);

            if (reply != null) {

                // update transport level metrics
                metrics.incrementMessagesReceived();
                try {
                    metrics.incrementBytesReceived(org.apache.axis2.transport.jms.jakarta.JMSUtils.getMessageSize(reply));
                } catch (jakarta.jms.JMSException e) {
                    log.warn("Error reading JMS message size to update transport metrics", e);
                }

                try {
                    jmsReplyMessage = processSyncResponseForJakarta(msgCtx, reply, contentTypeProperty);
                    metrics.incrementMessagesReceived();
                } catch (AxisFault e) {
                    metrics.incrementFaultsReceiving();
                    throw e;
                }

            } else {

                metrics.incrementFaultsReceiving();
                handleException("Did not receive a JMS response within " + timeout +
                        " ms to destination : " + replyDestination +
                        " with JMS correlation ID : " + correlationId);
            }

        } catch (jakarta.jms.JMSException e) {
            metrics.incrementFaultsReceiving();
            handleException("Error creating a consumer, or receiving a synchronous reply " +
                    "for outgoing MessageContext ID : " + msgCtx.getMessageID() +
                    " and reply Destination : " + replyDestination, e);
        } finally {
            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (jakarta.jms.JMSException e) {
                handleException(
                        "Unable to close consumer for reply destination: " + replyDestination, e);
            }
        }
        return jmsReplyMessage;
    }

    /**
     * Create a JMS Message from the given MessageContext and using the given
     * session
     *
     * @param msgContext the MessageContext
     * @param session    the JMS session
     * @param contentTypeProperty the message property to be used to store the
     *                            content type
     * @return a JMS message from the context and session
     * @throws JMSException on exception
     * @throws AxisFault on exception
     */
    private Message createJMSMessage(MessageContext msgContext, Session session,
            String contentTypeProperty) throws JMSException, AxisFault {

        Message message = null;
        String msgType = getProperty(msgContext, JMSConstants.JMS_MESSAGE_TYPE);

        // check the first element of the SOAP body, do we have content wrapped using the
        // default wrapper elements for binary (BaseConstants.DEFAULT_BINARY_WRAPPER) or
        // text (BaseConstants.DEFAULT_TEXT_WRAPPER) ? If so, do not create SOAP messages
        // for JMS but just get the payload in its native format
        String jmsPayloadType = guessMessageType(msgContext);

        if (jmsPayloadType == null) {

            OMOutputFormat format = BaseUtils.getOMOutputFormat(msgContext);
            MessageFormatter messageFormatter = null;
            try {
                messageFormatter = MessageProcessorSelector.getMessageFormatter(msgContext);
            } catch (AxisFault axisFault) {
                throw new JMSException("Unable to get the message formatter to use");
            }

            String contentType = messageFormatter.getContentType(
                msgContext, format, msgContext.getSoapAction());

            boolean useBytesMessage =
                msgType != null && JMSConstants.JMS_BYTE_MESSAGE.equals(msgType) ||
                    contentType.indexOf(HTTPConstants.HEADER_ACCEPT_MULTIPART_RELATED) > -1;

            OutputStream out;
            StringWriter sw;
            if (useBytesMessage) {
                BytesMessage bytesMsg = session.createBytesMessage();
                sw = null;
                out = new BytesMessageOutputStream(bytesMsg);
                message = bytesMsg;
            } else {
                sw = new StringWriter();
                try {
                    out = new WriterOutputStream(sw, format.getCharSetEncoding());
                } catch (UnsupportedCharsetException ex) {
                    handleException("Unsupported encoding " + format.getCharSetEncoding(), ex);
                    return null;
                }
            }
            
            try {
                messageFormatter.writeTo(msgContext, format, out, true);
                out.close();
            } catch (IOException e) {
                rollbackIfXATransaction(msgContext, false);
                handleException("IO Error while creating BytesMessage", e);
            }

            if (!useBytesMessage) {
                TextMessage txtMsg = session.createTextMessage();
                txtMsg.setText(sw.toString());
                message = txtMsg;
            }
            
            if (contentTypeProperty != null) {
                message.setStringProperty(contentTypeProperty, contentType);
            }

        } else if (JMSConstants.JMS_BYTE_MESSAGE.equals(jmsPayloadType)) {
            message = session.createBytesMessage();
            BytesMessage bytesMsg = (BytesMessage) message;
            OMElement wrapper = msgContext.getEnvelope().getBody().
                getFirstChildWithName(BaseConstants.DEFAULT_BINARY_WRAPPER);
            OMNode omNode = wrapper.getFirstOMChild();
            if (omNode != null && omNode instanceof OMText) {
                Object dh = ((OMText) omNode).getDataHandler();
                if (dh != null && dh instanceof DataHandler) {
                    try {
                        ((DataHandler) dh).writeTo(new BytesMessageOutputStream(bytesMsg));
                    } catch (IOException e) {
                        rollbackIfXATransaction(msgContext, false);
                        handleException("Error serializing binary content of element : " +
                            BaseConstants.DEFAULT_BINARY_WRAPPER, e);
                    }
                }
            }

        } else if (JMSConstants.JMS_TEXT_MESSAGE.equals(jmsPayloadType)) {
            message = session.createTextMessage();
            TextMessage txtMsg = (TextMessage) message;
            txtMsg.setText(msgContext.getEnvelope().getBody().
                getFirstChildWithName(BaseConstants.DEFAULT_TEXT_WRAPPER).getText());
        } else if (JMSConstants.JMS_MAP_MESSAGE.equalsIgnoreCase(jmsPayloadType)){
            message = session.createMapMessage();
            JMSUtils.convertXMLtoJMSMap(msgContext.getEnvelope().getBody().getFirstChildWithName(
                    JMSConstants.JMS_MAP_QNAME),(MapMessage)message);
        }

        // set the JMS correlation ID if specified
        String correlationId = getProperty(msgContext, JMSConstants.JMS_COORELATION_ID);
        if (correlationId == null && msgContext.getRelatesTo() != null) {
            correlationId = msgContext.getRelatesTo().getValue();
        }

        if (correlationId != null) {
            message.setJMSCorrelationID(correlationId);
        }

        if (msgContext.isServerSide()) {
            // set SOAP Action as a property on the JMS message
            setProperty(message, msgContext, BaseConstants.SOAPACTION);
        } else {
            String action = msgContext.getOptions().getAction();
            if (action != null) {
                message.setStringProperty(BaseConstants.SOAPACTION, action);
            }
        }

        JMSUtils.setTransportHeaders(msgContext, message);
        // set INTERNAL_TRANSACTION_COUNTED property in the JMS message if it is present in the messageContext
        setTransactionProperty(msgContext, message);
        return message;
    }

    /**
     * Create a JMS Message from the given MessageContext and using the given
     * session
     *
     * @param msgContext the MessageContext
     * @param session    the JMS session
     * @param contentTypeProperty the message property to be used to store the
     *                            content type
     * @return a JMS message from the context and session
     * @throws JMSException on exception
     * @throws AxisFault on exception
     */
    private jakarta.jms.Message createJMSJakartaMessage(MessageContext msgContext, jakarta.jms.Session session,
                                     String contentTypeProperty) throws jakarta.jms.JMSException, AxisFault {

        jakarta.jms.Message message = null;
        String msgType = getProperty(msgContext, JMSConstants.JMS_MESSAGE_TYPE);

        // check the first element of the SOAP body, do we have content wrapped using the
        // default wrapper elements for binary (BaseConstants.DEFAULT_BINARY_WRAPPER) or
        // text (BaseConstants.DEFAULT_TEXT_WRAPPER) ? If so, do not create SOAP messages
        // for JMS but just get the payload in its native format
        String jmsPayloadType = guessMessageType(msgContext);

        if (jmsPayloadType == null) {

            OMOutputFormat format = BaseUtils.getOMOutputFormat(msgContext);
            MessageFormatter messageFormatter = null;
            try {
                messageFormatter = MessageProcessorSelector.getMessageFormatter(msgContext);
            } catch (AxisFault axisFault) {
                throw new jakarta.jms.JMSException("Unable to get the message formatter to use");
            }

            String contentType = messageFormatter.getContentType(
                    msgContext, format, msgContext.getSoapAction());

            boolean useBytesMessage =
                    msgType != null && JMSConstants.JMS_BYTE_MESSAGE.equals(msgType) ||
                            contentType.indexOf(HTTPConstants.HEADER_ACCEPT_MULTIPART_RELATED) > -1;

            OutputStream out;
            StringWriter sw;
            if (useBytesMessage) {
                jakarta.jms.BytesMessage bytesMsg = session.createBytesMessage();
                sw = null;
                out = new org.apache.axis2.transport.jms.jakarta.iowrappers.BytesMessageOutputStream(bytesMsg);
                message = bytesMsg;
            } else {
                sw = new StringWriter();
                try {
                    out = new WriterOutputStream(sw, format.getCharSetEncoding());
                } catch (UnsupportedCharsetException ex) {
                    handleException("Unsupported encoding " + format.getCharSetEncoding(), ex);
                    return null;
                }
            }

            try {
                messageFormatter.writeTo(msgContext, format, out, true);
                out.close();
            } catch (IOException e) {
                rollbackIfXATransaction(msgContext, true);
                handleException("IO Error while creating BytesMessage", e);
            }

            if (!useBytesMessage) {
                jakarta.jms.TextMessage txtMsg = session.createTextMessage();
                txtMsg.setText(sw.toString());
                message = txtMsg;
            }

            if (contentTypeProperty != null) {
                message.setStringProperty(contentTypeProperty, contentType);
            }

        } else if (JMSConstants.JMS_BYTE_MESSAGE.equals(jmsPayloadType)) {
            message = session.createBytesMessage();
            BytesMessage bytesMsg = (BytesMessage) message;
            OMElement wrapper = msgContext.getEnvelope().getBody().
                    getFirstChildWithName(BaseConstants.DEFAULT_BINARY_WRAPPER);
            OMNode omNode = wrapper.getFirstOMChild();
            if (omNode != null && omNode instanceof OMText) {
                Object dh = ((OMText) omNode).getDataHandler();
                if (dh != null && dh instanceof DataHandler) {
                    try {
                        ((DataHandler) dh).writeTo(new BytesMessageOutputStream(bytesMsg));
                    } catch (IOException e) {
                        rollbackIfXATransaction(msgContext, true);
                        handleException("Error serializing binary content of element : " +
                                BaseConstants.DEFAULT_BINARY_WRAPPER, e);
                    }
                }
            }

        } else if (JMSConstants.JMS_TEXT_MESSAGE.equals(jmsPayloadType)) {
            message = session.createTextMessage();
            jakarta.jms.TextMessage txtMsg = (jakarta.jms.TextMessage) message;
            txtMsg.setText(msgContext.getEnvelope().getBody().
                    getFirstChildWithName(BaseConstants.DEFAULT_TEXT_WRAPPER).getText());
        } else if (JMSConstants.JMS_MAP_MESSAGE.equalsIgnoreCase(jmsPayloadType)){
            message = session.createMapMessage();
            org.apache.axis2.transport.jms.jakarta.JMSUtils.convertXMLtoJMSMap(msgContext.getEnvelope().getBody().getFirstChildWithName(
                    JMSConstants.JMS_MAP_QNAME),(jakarta.jms.MapMessage)message);
        }

        // set the JMS correlation ID if specified
        String correlationId = getProperty(msgContext, JMSConstants.JMS_COORELATION_ID);
        if (correlationId == null && msgContext.getRelatesTo() != null) {
            correlationId = msgContext.getRelatesTo().getValue();
        }

        if (correlationId != null) {
            message.setJMSCorrelationID(correlationId);
        }

        if (msgContext.isServerSide()) {
            // set SOAP Action as a property on the JMS message
            setJakartaProperty(message, msgContext, BaseConstants.SOAPACTION);
        } else {
            String action = msgContext.getOptions().getAction();
            if (action != null) {
                message.setStringProperty(BaseConstants.SOAPACTION, action);
            }
        }

        org.apache.axis2.transport.jms.jakarta.JMSUtils.setTransportHeaders(msgContext, message);
        // set INTERNAL_TRANSACTION_COUNTED property in the JMS message if it is present in the messageContext
        setJakartaTransactionProperty(msgContext, message);
        return message;
    }

    /**
     * Guess the message type to use for JMS looking at the message contexts' envelope
     * @param msgContext the message context
     * @return JMSConstants.JMS_BYTE_MESSAGE or JMSConstants.JMS_TEXT_MESSAGE or null
     */
    private String guessMessageType(MessageContext msgContext) {
        OMElement firstChild = msgContext.getEnvelope().getBody().getFirstElement();
        if (firstChild != null) {
            if (BaseConstants.DEFAULT_BINARY_WRAPPER.equals(firstChild.getQName())) {
                return JMSConstants.JMS_BYTE_MESSAGE;
            } else if (BaseConstants.DEFAULT_TEXT_WRAPPER.equals(firstChild.getQName())) {
                return JMSConstants.JMS_TEXT_MESSAGE;
            } else if (JMSConstants.JMS_MAP_QNAME.equals(firstChild.getQName())){
                return  JMSConstants.JMS_MAP_MESSAGE;
            }
        }
        return null;
    }

    /**
     * Creates an Axis MessageContext for the received JMS message and
     * sets up the transports and various properties
     *
     * @param outMsgCtx           the outgoing message for which we are expecting the response
     * @param message             the JMS response message received
     * @param contentTypeProperty the message property used to determine the content type
     *                            of the response message
     * @return Bean containing response message information.
     * @throws AxisFault on error
     */
    private JMSReplyMessage processSyncResponse(MessageContext outMsgCtx, Message message,
                                                String contentTypeProperty) throws AxisFault {

        MessageContext responseMsgCtx = createResponseMessageContext(outMsgCtx);

        // load any transport headers from received message
        JMSUtils.loadTransportHeaders(message, responseMsgCtx);

        String contentType = (contentTypeProperty == null) ? (String) outMsgCtx.getProperty(JMSConstants.CONTENT_TYPE)
                : JMSUtils.getProperty(message, contentTypeProperty);

        try {
            JMSUtils.setSOAPEnvelope(message, responseMsgCtx, contentType);
        } catch (JMSException ex) {
            throw AxisFault.makeFault(ex);
        }

        JMSReplyMessage jmsReplyMessage = new JMSReplyMessage();
        jmsReplyMessage.setMsgctx(responseMsgCtx);
        jmsReplyMessage.setTransportHeaders(JMSUtils.getTransportHeaders(message, responseMsgCtx));
        jmsReplyMessage.setSoapAction(JMSUtils.getProperty(message, BaseConstants.SOAPACTION));
        jmsReplyMessage.setContentType(contentType);
        return jmsReplyMessage;
    }

    /**
     * Creates an Axis MessageContext for the received JMS message and
     * sets up the transports and various properties
     *
     * @param outMsgCtx           the outgoing message for which we are expecting the response
     * @param message             the JMS response message received
     * @param contentTypeProperty the message property used to determine the content type
     *                            of the response message
     * @return Bean containing response message information.
     * @throws AxisFault on error
     */
    private org.apache.axis2.transport.jms.jakarta.JMSReplyMessage processSyncResponseForJakarta(MessageContext outMsgCtx, jakarta.jms.Message message,
                                                String contentTypeProperty) throws AxisFault {

        MessageContext responseMsgCtx = createResponseMessageContext(outMsgCtx);

        // load any transport headers from received message
        org.apache.axis2.transport.jms.jakarta.JMSUtils.loadTransportHeaders(message, responseMsgCtx);

        String contentType = (contentTypeProperty == null) ? (String) outMsgCtx.getProperty(JMSConstants.CONTENT_TYPE)
                : org.apache.axis2.transport.jms.jakarta.JMSUtils.getProperty(message, contentTypeProperty);

        try {
            org.apache.axis2.transport.jms.jakarta.JMSUtils.setSOAPEnvelope(message, responseMsgCtx, contentType);
        } catch (jakarta.jms.JMSException ex) {
            throw AxisFault.makeFault(ex);
        }

        org.apache.axis2.transport.jms.jakarta.JMSReplyMessage jmsReplyMessage = new org.apache.axis2.transport.jms.jakarta.JMSReplyMessage();
        jmsReplyMessage.setMsgctx(responseMsgCtx);
        jmsReplyMessage.setTransportHeaders(org.apache.axis2.transport.jms.jakarta.JMSUtils.getTransportHeaders(
                message, responseMsgCtx));
        jmsReplyMessage.setSoapAction(org.apache.axis2.transport.jms.jakarta.JMSUtils.getProperty(
                message, BaseConstants.SOAPACTION));
        jmsReplyMessage.setContentType(contentType);
        return jmsReplyMessage;
    }

    /**
     * Set the "INTERNAL_TRANSACTION_COUNTED" property in the message if it is present in the message context.
     *
     * @param msgContext the message context.
     * @param message the JMS message.
     */
    private void setTransactionProperty(MessageContext msgContext, Message message) {
        Object transactionProperty = msgContext.getProperty(BaseConstants.INTERNAL_TRANSACTION_COUNTED);
        if (transactionProperty instanceof Boolean) {
            try {
                message.setBooleanProperty(BaseConstants.INTERNAL_TRANSACTION_COUNTED, (Boolean) transactionProperty);
            } catch (JMSException e) {
                log.warn("Couldn't set message property : " + BaseConstants.INTERNAL_TRANSACTION_COUNTED + " = "
                                 + transactionProperty, e);
            }
        }
    }

    /**
     * Set the "INTERNAL_TRANSACTION_COUNTED" property in the message if it is present in the message context.
     *
     * @param msgContext the message context.
     * @param message the JMS message.
     */
    private void setJakartaTransactionProperty(MessageContext msgContext, jakarta.jms.Message message) {
        Object transactionProperty = msgContext.getProperty(BaseConstants.INTERNAL_TRANSACTION_COUNTED);
        if (transactionProperty instanceof Boolean) {
            try {
                message.setBooleanProperty(BaseConstants.INTERNAL_TRANSACTION_COUNTED, (Boolean) transactionProperty);
            } catch (jakarta.jms.JMSException e) {
                log.warn("Couldn't set message property : " + BaseConstants.INTERNAL_TRANSACTION_COUNTED + " = "
                        + transactionProperty, e);
            }
        }
    }

    private void setProperty(Message message, MessageContext msgCtx, String key) {

        String value = getProperty(msgCtx, key);
        if (value != null) {
            try {
                message.setStringProperty(key, value);
            } catch (JMSException e) {
                log.warn("Couldn't set message property : " + key + " = " + value, e);
            }
        }
    }

    private void setJakartaProperty(jakarta.jms.Message message, MessageContext msgCtx, String key) {

        String value = getProperty(msgCtx, key);
        if (value != null) {
            try {
                message.setStringProperty(key, value);
            } catch (jakarta.jms.JMSException e) {
                log.warn("Couldn't set message property : " + key + " = " + value, e);
            }
        }
    }

    private String getProperty(MessageContext mc, String key) {
        return (String) mc.getProperty(key);
    }
    
    public void clearActiveConnections(){
    	log.error("Not Implemented.");
    }

    private void commitXAJavaxTransaction(Transaction transaction) throws AxisFault {
        ArrayList<JMSMessageSender> msgSenderList = (ArrayList) jmsMessageSenderMap.get(transaction);
        if (msgSenderList.size() > 0) {
            for (JMSMessageSender msgSender : msgSenderList) {
                try {
                    msgSender.getJmsXaResource().end(msgSender.getJmsXAXid(), XAResource.TMSUCCESS);
                    msgSender.getJmsXaResource().prepare(msgSender.getJmsXAXid());
                    msgSender.getJmsXaResource().commit(msgSender.getJmsXAXid(), false);
                    msgSender.close();
                } catch (XAException e) {
                    handleException("Error occurred during rolling back transaction", e);
                    msgSender.closeOnException();
                }

            }
            jmsMessageSenderMap.remove(transaction);
        }
    }

    private void commitXAJakartaTransaction(Transaction transaction) throws AxisFault {
        ArrayList<org.apache.axis2.transport.jms.jakarta.JMSMessageSender> msgSenderList =
                (ArrayList) jakartaMessageSenderMap.get(transaction);
        if (msgSenderList.size() > 0) {
            for (org.apache.axis2.transport.jms.jakarta.JMSMessageSender msgSender : msgSenderList) {
                try {
                    msgSender.getJmsXaResource().end(msgSender.getJmsXAXid(), XAResource.TMSUCCESS);
                    msgSender.getJmsXaResource().prepare(msgSender.getJmsXAXid());
                    msgSender.getJmsXaResource().commit(msgSender.getJmsXAXid(), false);
                    msgSender.close();
                } catch (XAException e) {
                    handleException("Error occurred during rolling back transaction", e);
                    msgSender.closeOnException();
                }

            }
            jakartaMessageSenderMap.remove(transaction);
        }
    }

    private void rollbackJavaxXATransaction(Transaction transaction) throws AxisFault {
        ArrayList<JMSMessageSender> msgSenderList = (ArrayList) jmsMessageSenderMap.get(transaction);
        if (msgSenderList.size() > 0) {
            for (JMSMessageSender msgSender : msgSenderList) {
                try {
                    msgSender.getJmsXaResource().end(msgSender.getJmsXAXid(), XAResource.TMSUCCESS);
                    msgSender.getJmsXaResource().prepare(msgSender.getJmsXAXid());
                    msgSender.getJmsXaResource().commit(msgSender.getJmsXAXid(), false);
                    msgSender.close();
                } catch (XAException e) {
                    handleException("Error occurred during rolling back transaction", e);
                    msgSender.closeOnException();
                }
            }
            jmsMessageSenderMap.remove(transaction);
        }
    }

    private void rollbackJakartaXATransaction(Transaction transaction) throws AxisFault {
        ArrayList<org.apache.axis2.transport.jms.jakarta.JMSMessageSender> msgSenderList =
                (ArrayList) jakartaMessageSenderMap.get(transaction);
        if (msgSenderList.size() > 0) {
            for (org.apache.axis2.transport.jms.jakarta.JMSMessageSender msgSender : msgSenderList) {
                try {
                    msgSender.getJmsXaResource().end(msgSender.getJmsXAXid(), XAResource.TMSUCCESS);
                    msgSender.getJmsXaResource().prepare(msgSender.getJmsXAXid());
                    msgSender.getJmsXaResource().commit(msgSender.getJmsXAXid(), false);
                    msgSender.close();
                } catch (XAException e) {
                    handleException("Error occurred during rolling back transaction", e);
                    msgSender.closeOnException();
                }
            }
            jakartaMessageSenderMap.remove(transaction);
        }
    }
}

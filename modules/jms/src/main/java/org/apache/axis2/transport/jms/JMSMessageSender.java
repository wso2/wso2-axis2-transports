/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.axis2.transport.jms;

import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.transaction.Transaction;
import javax.transaction.UserTransaction;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Performs the actual sending of a JMS message, and the subsequent committing of a JTA transaction
 * (if requested) or the local session transaction, if used. An instance of this class is unique
 * to a single message send out operation and will not be shared.
 */
public class JMSMessageSender {

    private static final Log log = LogFactory.getLog(JMSMessageSender.class);

    /** The Connection to be used to send out */
    private Connection connection = null;
    /** The Session to be used to send out */
    private Session session = null;
    /** The MessageProducer used */
    private MessageProducer producer = null;
    /** Target Destination */
    private Destination destination = null;
    /** The level of cachability for resources */
    private int cacheLevel = JMSConstants.CACHE_CONNECTION;
    /** JMS spec version (1.0.2b or 1.1 or 2.0) default is 1.0.2b */
    private String jmsSpecVersion = null;
    /** Are we sending to a Queue ? */
    private Boolean isQueue = null;
    private Boolean jtaCommit;
    private Boolean rollbackOnly;
    private boolean sendingSuccessful;
    private SessionWrapper sessionWrapper;

    /**
     +     * Boolean to track if producer caching will be honoured.
     +     * True if producer caching is enabled and this {@link JMSMessageSender} is created specifying a
     +     * {@link JMSConnectionFactory} and target EPR, via
     +     * {@link JMSMessageSender#JMSMessageSender(JMSConnectionFactory, String)}.
     +     */
    private Boolean isProducerCachingHonoured = false;

    private Xid jmsXAXid;
    private XAResource jmsXaResource;
    private Transaction jmsTransaction;

    /**
     * This is a low-end method to support the one-time sends using JMS 1.0.2b
     * @param connection the JMS Connection
     * @param session JMS Session
     * @param producer the MessageProducer
     * @param destination the JMS Destination
     * @param cacheLevel cacheLevel - None | Connection | Session | Producer
     * @param jmsSpecVersion JMS spec version set to (1.0.2b)
     * @param isQueue posting to a Queue?
     */
    public JMSMessageSender(Connection connection, Session session, MessageProducer producer,
        Destination destination, int cacheLevel, String jmsSpecVersion, Boolean isQueue) {

        this.connection = connection;
        this.session = session;
        this.producer = producer;
        this.destination = destination;
        this.cacheLevel = cacheLevel;
        this.jmsSpecVersion = jmsSpecVersion;
        this.isQueue = isQueue;
    }

    /**
     * This is a low-end method to support the one-time sends using JMS 1.0.2b
     *
     * @param connection     the JMS Connection
     * @param session        JMS Session
     * @param producer       the MessageProducer
     * @param destination    the JMS Destination
     * @param cacheLevel     cacheLevel - None | Connection | Session | Producer
     * @param jmsSpecVersion JMS spec version set to (1.0.2b)
     * @param isQueue        posting to a Queue?
     */
    public JMSMessageSender(Connection connection, Session session, MessageProducer producer,
                            Destination destination, int cacheLevel, String jmsSpecVersion, Boolean isQueue, Transaction tx, Xid xid, XAResource xaResource) {

        this.connection = connection;
        this.session = session;
        this.producer = producer;
        this.destination = destination;
        this.cacheLevel = cacheLevel;
        this.jmsSpecVersion = jmsSpecVersion;
        this.isQueue = isQueue;
        this.jmsTransaction = tx;
        this.jmsXAXid = xid;
        this.jmsXaResource = xaResource;
    }

    /**
     * Create a JMSSender using a JMSConnectionFactory and target EPR
     *
     * @param jmsConnectionFactory the JMSConnectionFactory
     * @param targetAddress target EPR
     */
    public JMSMessageSender(JMSConnectionFactory jmsConnectionFactory, String targetAddress) {

        try {
            this.cacheLevel = jmsConnectionFactory.getCacheLevel();
            this.isProducerCachingHonoured = cacheLevel > JMSConstants.CACHE_SESSION;
            this.jmsSpecVersion = jmsConnectionFactory.jmsSpecVersion();
            this.connection = jmsConnectionFactory.getConnection();
            this.sessionWrapper = jmsConnectionFactory.getSessionWrapper(connection);
            this.session = sessionWrapper.getSession();
            boolean isQueue = jmsConnectionFactory.isQueue() == null ? true : jmsConnectionFactory.isQueue();
            String destinationFromAddress = JMSUtils.getDestination(targetAddress);
            //precedence is given to the destination specified by targetAddress
            if (destinationFromAddress != null && !destinationFromAddress.isEmpty()) {
                this.destination = jmsConnectionFactory.getDestination(JMSUtils.getDestination(targetAddress),
                        isQueue ? JMSConstants.DESTINATION_TYPE_QUEUE : JMSConstants.DESTINATION_TYPE_TOPIC);
            } else {
                this.destination = jmsConnectionFactory.getSharedDestination();
            }
            this.producer = jmsConnectionFactory.getMessageProducer(connection, sessionWrapper, destination);
        } catch (Exception e) {
            handleException("Error while creating message sender", e);
        }
    }

    /**
     * Perform actual send of JMS message to the Destination selected.
     *
     * @param message the JMS message
     * @param msgCtx the Axis2 MessageContext
     */
    public void send(Message message, MessageContext msgCtx) throws JMSException {

        jtaCommit = getBooleanProperty(msgCtx, BaseConstants.JTA_COMMIT_AFTER_SEND);
        rollbackOnly = getBooleanProperty(msgCtx, BaseConstants.SET_ROLLBACK_ONLY);
        String deliveryMode = getStringProperty(msgCtx, JMSConstants.JMS_DELIVERY_MODE);
        Integer priority = getIntegerProperty(msgCtx, JMSConstants.JMS_PRIORITY);
        Integer timeToLive = getIntegerProperty(msgCtx, JMSConstants.JMS_TIME_TO_LIVE);
        Integer messageDelay = getIntegerProperty(msgCtx, JMSConstants.JMS_MESSAGE_DELAY);

        // Do not commit, if message is marked for rollback
        if (rollbackOnly != null && rollbackOnly) {
            jtaCommit = Boolean.FALSE;
        }

        if (deliveryMode != null) {
            try {
                if (JMSConstants.JMS_PERSISTENT_DELIVERY_MODE.equalsIgnoreCase(deliveryMode)) {
                    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                } else if (JMSConstants.JMS_NON_PERSISTENT_DELIVERY_MODE.equalsIgnoreCase(deliveryMode)) {
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                } else {
                    //Warn user since unknown value for JMS_DELIVERY_MODE property
                    log.warn("Unknown JMS Delivery mode: " + deliveryMode + " defined as JMS_DELIVERY_MODE property");
                }
            } catch (JMSException e) {
                handleException("Error setting JMS Producer for PERSISTENT delivery", e);
            }
        }
        if (priority != null) {
            try {
                producer.setPriority(priority);
            } catch (JMSException e) {
                handleException("Error setting JMS Producer priority to : " + priority, e);
            }
        }
        if (timeToLive != null) {
            try {
                producer.setTimeToLive(timeToLive);
            } catch (JMSException e) {
                handleException("Error setting JMS Producer TTL to : " + timeToLive, e);
            }
        }
        /** JMS 2.0 feature : Message Delay time interval
         *  This delay will be applied when the message is sent from Broker to consumers
         * */
        if ("2.0".equals(jmsSpecVersion) && messageDelay != null) {
            try {
                producer.setDeliveryDelay(messageDelay);
            } catch (JMSException e) {
                handleException("Error setting JMS Producer Message Delivery Delay : " + messageDelay, e);
            }
        }

        sendingSuccessful = false;
        // perform actual message sending
        if ("1.1".equals(jmsSpecVersion) || "2.0".equals(jmsSpecVersion) || isQueue == null) {
            if (isProducerCachingHonoured) {
                producer.send(destination, message);
            } else {
                producer.send(message);
            }
        } else {
            if (isQueue) {
                try {
                    if (isProducerCachingHonoured) {
                        (producer).send(destination, message);
                    } else {
                        (producer).send(message);
                    }
                } catch (JMSException e) {
                    log.error(("Error sending message with MessageContext ID : " + msgCtx.getMessageID()
                            + " to destination : " + destination + " Hence creating a temporary subscriber" + e));
                    createTempQueueConsumer();
                    if (isProducerCachingHonoured) {
                        producer.send(destination, message);
                    } else {
                        producer.send(message);
                    }
                }
            } else {
                try {
                    if (isProducerCachingHonoured) {
                        ((TopicPublisher) producer).publish((Topic) destination, message);
                    } else {
                        ((TopicPublisher) producer).publish(message);
                    }
                } catch (JMSException e) {
                    createTempTopicSubscriber();
                    if (isProducerCachingHonoured) {
                        ((TopicPublisher) producer).publish((Topic) destination, message);
                    } else {
                        ((TopicPublisher) producer).publish(message);
                    }
                }
            }
        }

        // set the actual MessageID to the message context for use by any others down the line
        String msgId = null;
        try {
            msgId = message.getJMSMessageID();
            if (msgId != null) {
                msgCtx.setProperty(JMSConstants.JMS_MESSAGE_ID, msgId);
            }
        } catch (JMSException ignore) {
        }

        sendingSuccessful = true;

        if (log.isDebugEnabled()) {
            log.debug("Sent Message Context ID : " + msgCtx.getMessageID() +
                    " with JMS Message ID : " + msgId
                    + " to destination : " + destination);
        }
    }

    /**
     * Perform actual send of JMS message to the Destination selected and commit the transaction on
     * successful scenario, rollback on failure scenario.
     *
     * @param message the JMS message
     * @param msgCtx  the Axis2 MessageContext
     */
    public void transactionallySend(Message message, MessageContext msgCtx) {

        try {
            send(message, msgCtx);

        } catch (JMSException e) {
            handleException("Error sending message with MessageContext ID : " +
                    msgCtx.getMessageID() + " to destination : " + destination, e);

        } finally {

            if (jtaCommit != null) {
                UserTransaction ut = (UserTransaction) msgCtx.getProperty(BaseConstants.USER_TRANSACTION);
                if (ut != null) {
                    try {
                        if (sendingSuccessful && jtaCommit) {
                            ut.commit();
                        } else {
                            ut.rollback();
                        }
                        msgCtx.removeProperty(BaseConstants.USER_TRANSACTION);

                        if (log.isDebugEnabled()) {
                            log.debug((sendingSuccessful ? "Committed" : "Rolled back") +
                                    " JTA Transaction");
                        }
                    } catch (Exception e) {
                        handleException("Error committing/rolling back JTA transaction after " +
                                "sending of message with MessageContext ID : " + msgCtx.getMessageID() +
                                " to destination : " + destination, e);
                    }
                }
            } else {
                try {
                    if (session.getTransacted()) {
                        if (sendingSuccessful && (rollbackOnly == null || !rollbackOnly)) {
                            session.commit();
                        } else {
                            session.rollback();
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug((sendingSuccessful ? "Committed" : "Rolled back") +
                                " local (JMS Session) Transaction");
                    }
                } catch (JMSException e) {
                    handleException("Error committing/rolling back local (i.e. session) " +
                            "transaction after sending of message with MessageContext ID : " +
                            msgCtx.getMessageID() + " to destination : " + destination, e);
                }
            }
        }
    }

    /**
     * Creating a temporary  Queue Consumer; The objective of this is to make
     * a binding for this destination in the message broker. If there is no
     * bindings created in the server before sending messages, messages will not
     * be stored in the server. So we create a consumer and close it, if there
     * are not any bindings already created in the server
     *
     * */
    public void createTempQueueConsumer() throws JMSException {
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.close();
    }

    public void createTempTopicSubscriber() throws JMSException {
        TopicSubscriber subscriber = ((TopicSession) session).createSubscriber((Topic) destination);
        subscriber.close();
    }
    /**
     * Close non-shared producer, session and connection if any
     */
    public void close() {
        if (producer != null && cacheLevel < JMSConstants.CACHE_PRODUCER) {
            try {
                producer.close();
            } catch (JMSException e) {
                log.error("Error closing JMS MessageProducer after send", e);
            } finally {
                producer = null;
            }
        }

        if (session != null && cacheLevel < JMSConstants.CACHE_SESSION) {
            try {
                session.close();
            } catch (JMSException e) {
                log.error("Error closing JMS Session after send", e);
            } finally {
                session = null;
            }
        }

        if (connection != null && cacheLevel < JMSConstants.CACHE_CONNECTION) {
            try {
                connection.close();
            } catch (JMSException e) {
                log.error("Error closing JMS Connection after send", e);
            } finally {
                connection = null;
            }
        }
    }

    private void handleException(String message, Exception e) {
        log.error(message, e);

        // Cleanup connections on error. See ESBJAVA-4713
        if (!isTransacted()) { // else transaction rollback will call closeOnException() due to exception thrown below
            if (log.isDebugEnabled()) {
                log.debug("Cleaning up connections on error", e);
            }
            closeOnException();
        }

        throw new AxisJMSException(message, e);
    }

    /**
     * Close connection upon exception. See ESBJAVA-4713
     */
    public void closeOnException() {
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException e) {
                log.error("Error closing JMS MessageProducer after send", e);
            } finally {
                producer = null;
            }
        }

        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
                log.error("Error closing JMS Session after send", e);
            } finally {
                session = null;
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                log.error("Error closing JMS Connection after send", e);
            } finally {
                connection = null;
            }
        }
    }

    private boolean isTransacted() {
        return (jmsXAXid != null || jmsXaResource != null || jmsTransaction != null);
    }

    private Boolean getBooleanProperty(MessageContext msgCtx, String name) {
        Object o = msgCtx.getProperty(name);
        if (o != null) {
            if (o instanceof Boolean) {
                return (Boolean) o;
            } else if (o instanceof String) {
                return Boolean.valueOf((String) o);
            }
        }
        return null;
    }

    private String getStringProperty(MessageContext msgCtx, String name) {
        Object propObject = msgCtx.getProperty(name);
        if (propObject != null && propObject instanceof String) {
            return (String) propObject;
        }
        return null;
    }

    private Integer getIntegerProperty(MessageContext msgCtx, String name) {
        Object o = msgCtx.getProperty(name);
        if (o != null) {
            if (o instanceof Integer) {
                return (Integer) o;
            } else if (o instanceof String) {
                return Integer.parseInt((String) o);
            }
        }
        return null;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public void setProducer(MessageProducer producer) {
        this.producer = producer;
    }

    public void setCacheLevel(int cacheLevel) {
        this.cacheLevel = cacheLevel;
    }

    public int getCacheLevel() {
        return cacheLevel;
    }

    public Connection getConnection() {
        return connection;
    }

    public MessageProducer getProducer() {
        return producer;
    }

    public Session getSession() {
        return session;
    }

    public Xid getJmsXAXid() {
        return jmsXAXid;
    }

    public void setJmsXAXid(Xid jmsXAXid) {
        this.jmsXAXid = jmsXAXid;
    }

    public XAResource getJmsXaResource() {
        return jmsXaResource;
    }

    public void setJmsXaResource(XAResource jmsXaResource) {
        this.jmsXaResource = jmsXaResource;
    }

    public Transaction getJmsTransaction() {
        return jmsTransaction;
    }

    public void setJmsTransaction(Transaction jmsTransaction) {
        this.jmsTransaction = jmsTransaction;
    }

    public Destination getDestination() {
        return destination;
    }

    public SessionWrapper getSessionWrapper() {
        return sessionWrapper;
    }
}

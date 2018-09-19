/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.jms.dualchannel;

import org.apache.axis2.transport.jms.JMSConstants;
import org.apache.axis2.transport.jms.JMSUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Data holder for a cached subscription meant for consuming reply messages. Once a new request arrives for the
 * relevant proxy, the JMSSender will attach a @{@link JMSReplyContainer} to this subscription with its
 * request correlation Id. When the subscription receives a matching reply, it will notify the JMSSender.
 */
public class JMSReplySubscription implements Runnable {

    private static final Log log = LogFactory.getLog(JMSReplySubscription.class);
    private static final int CONSUME_TIMEOUT = 1000;

    /**
     * Broker specific message consumer, connection, session references.
     */
    private MessageConsumer messageConsumer;
    private Connection connection;
    private Session session;

    /**
     * Map for maintaining active requests for the relevant proxy.
     */
    private ConcurrentHashMap<String, JMSReplyContainer> listeningRequests;

    /**
     * Reference to periodic task which triggers a consumer.receive.
     */
    private ScheduledFuture taskReference;

    /**
     * Unique identifier for this subscription based on proxy name, server IP and queue name.
     */
    private String identifier;

    /**
     * Temporary queue on which this subscription is listening for the replies.
     */
    private TemporaryQueue temporaryQueue;

    /**
     * Lock to ensure that updates to the subscription / its listeners are updated consistently.
     */
    private Lock lock = new ReentrantLock();

    JMSReplySubscription(InitialContext initialContext, String connectionFactoryName, String identifier)
            throws NamingException, JMSException {

        this.identifier = identifier;
        listeningRequests = new ConcurrentHashMap<>();

        ConnectionFactory connectionFactory = JMSUtils.lookup(initialContext, ConnectionFactory.class, connectionFactoryName);

        String username = null;
        String password = null;

        if (initialContext.getEnvironment().containsKey(JMSConstants.PARAM_JMS_PASSWORD)) {
            username = (String) initialContext.getEnvironment().get(JMSConstants.PARAM_JMS_USERNAME);
            password = (String) initialContext.getEnvironment().get(JMSConstants.PARAM_JMS_PASSWORD);
        }

        //todo fix.
        connection = JMSUtils.createConnection(connectionFactory, username, password, JMSConstants.JMS_SPEC_VERSION_2_0,
                true, false, null, false);
        connection.setExceptionListener(new ReplySubscriptionExceptionListener(identifier));
        connection.start();

        session = JMSUtils.createSession(connection, false, Session.AUTO_ACKNOWLEDGE, JMSConstants.JMS_SPEC_VERSION_2_0, true);

        temporaryQueue = session.createTemporaryQueue();

        messageConsumer = JMSUtils.createConsumer(session, temporaryQueue, "");

    }

    /**
     * After sending a JMS message with a ReplyTo header, the JMSSender can register a listener here to be notified
     * of the response.
     * @param jmsCorrelationId correlation ID used to correlate the request to response message.
     * @param replyContainer Object used to listen to, and retrieve the response message.
     */
    public void registerListener(String jmsCorrelationId, JMSReplyContainer replyContainer) {
        listeningRequests.put(jmsCorrelationId, replyContainer);
    }

    /**
     * Remove listener to a specific JMS request.
     * @param jmsCorrelationId correlation ID used to correlate the request to response message.
     */
    public void unregisterListener(String jmsCorrelationId) {
        listeningRequests.remove(jmsCorrelationId);
    }

    public TemporaryQueue getTemporaryQueue() {
        return temporaryQueue;
    }

    @Override
    public void run() {
        try {
            Message message = messageConsumer.receive(CONSUME_TIMEOUT);

            while (null != message) {

                String jmsCorrelationId = message.getJMSCorrelationID();
                if (log.isDebugEnabled()) {
                    log.debug("Received correlationId : " + jmsCorrelationId + " identifier : " + identifier);
                }
                JMSReplyContainer jmsReplyContainer = listeningRequests.get(jmsCorrelationId);

                if (null != jmsReplyContainer) {
                    jmsReplyContainer.setMessage(message);
                    jmsReplyContainer.getCountDownLatch().countDown();
                }

                message = messageConsumer.receive(CONSUME_TIMEOUT);
            }

        } catch (JMSException e) {
            log.error("Error while receiving message : ", e);
        }
    }


    /**
     * Maintain reference to ScheduledFuture running the scheduled task.
     * @param taskReference reference to ScheduledFuture running the scheduled task
     */
    void setTaskReference(ScheduledFuture taskReference) {
        this.taskReference = taskReference;
    }

    /**
     * Announce closing of this subscription to any listeners still waiting for a reply, and cancel the scheduled task.
     */
    void cleanupTask() {

        // No locks required since this is a concurrent hash map.
        for (Map.Entry<String,JMSReplyContainer> request : listeningRequests.entrySet()) {
            request.getValue().getCountDownLatch().countDown();
        }
        taskReference.cancel(true);

        if (log.isDebugEnabled()) {
            log.debug(" Cancelled task for reply subscription identified with : " + identifier);
        }

        lock.lock();
        try {
            messageConsumer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            log.error("Error when trying to close JMS reply subscription for identifier : " + identifier, e);
        } finally {
            messageConsumer = null;
            session = null;
            connection = null;
        }

        lock.unlock();

    }

}
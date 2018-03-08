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

package org.apache.axis2.transport.oracleaq;

import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.NotSupportedException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

/**
 * Each service will have one ServiceTaskManager instance that will create, manage and also destroy
 * idle tasks created for it, for message receipt. This will also allow individual tasks to cache
 * the Connection, Session or Consumer as necessary, considering the transactionality required and
 * user preference.
 *
 * This also acts as the ExceptionListener for all JMS connections made on behalf of the service.
 * Since the ExceptionListener is notified by a JMS provider on a "serious" error, we simply try
 * to re-connect. Thus a connection failure for a single task, will re-initialize the state afresh
 * for the service, by discarding all connections. 
 */
public class ServiceTaskManager {

    /** The logger */
    private static final Log log = LogFactory.getLog(ServiceTaskManager.class);

    /** The Task manager is stopped or has not started */
    private static final int STATE_STOPPED = 0;
    /** The Task manager is started and active */
    private static final int STATE_STARTED = 1;
    /** The Task manager is paused temporarily */
    private static final int STATE_PAUSED = 2;
    /** The Task manager is started, but a shutdown has been requested */
    private static final int STATE_SHUTTING_DOWN = 3;
    /** The Task manager has encountered an error */
    private static final int STATE_FAILURE = 4;

    /** The name of the service managed by this instance */
    private String serviceName;
    /** The ConnectionFactory MUST refer to an XAConnectionFactory to use JTA */
    private String connFactoryJNDIName;
    /** The JNDI name of the Destination Queue or Topic */
    // TODO: this overlaps with JMSEndpoint#jndiDestinationName; needs to be clarified
    private String destinationJNDIName;
    /** JNDI location for the JTA UserTransaction */
    private String userTransactionJNDIName = "java:comp/UserTransaction";
    /** The type of destination - P2P or PubSub (or JMS 1.1 API generic?) */
    // TODO: this overlaps with JMSEndpoint#destinationType; needs to be clarified
    private int destinationType = JMSConstants.GENERIC;
    /** An optional message selector */
    private String messageSelector = null;

    /** Should tasks run without transactions, using transacted Sessions (i.e. local), or JTA */
    private int transactionality = BaseConstants.TRANSACTION_NONE;
    /** Should created Sessions be transactional ? - should be false when using JTA */
    private boolean sessionTransacted = true;
    /** Session acknowledgement mode when transacted Sessions (i.e. local transactions) are used */
    private int sessionAckMode = Session.AUTO_ACKNOWLEDGE;

    /** Is the subscription durable ? */
    private boolean subscriptionDurable = false;
    /** The name of the durable subscriber for this client */
    private String durableSubscriberName = null;
    /** In PubSub mode, should I receive messages sent by me / my connection ? */
    private boolean pubSubNoLocal = false;
    /** Number of concurrent consumers - for PubSub, this should be 1 to prevent multiple receipt */
    private int concurrentConsumers = 1;
    /** Maximum number of consumers to create - see @concurrentConsumers */
    private int maxConcurrentConsumers = 1;
    /** The number of idle (i.e. message-less) attempts to be tried before suicide, to scale down */
    private int idleTaskExecutionLimit = 10;
    /** The maximum number of successful message receipts for a task - to limit thread life span */
    private int maxMessagesPerTask = -1;    // default is unlimited
    /** The default receive timeout - a negative value means wait forever, zero dont wait at all */
    private int receiveTimeout = 1000;
    /** JMS Resource cache level - Connection, Session, Consumer. Auto will select safe default */
    private int cacheLevel = JMSConstants.CACHE_AUTO;
    /** Should we cache the UserTransaction handle from JNDI - true for almost all app servers */
    private boolean cacheUserTransaction = true;
    /** Shared UserTransactionHandle */
    private UserTransaction sharedUserTransaction = null;
    /** JMS Spec (default is 1.1) */
    private String jmsSpec = JMSConstants.JMS_SPEC_VERSION_1_0;

    /** Initial duration to attempt re-connection to JMS provider after failure */
    private int initialReconnectDuration = 10000;
    /**
     * Default duration to re-attempt after message consume failure
     */
    private Integer consumeErrorRetryDelay = 100;
    /** Progression factory for geometric series that calculates re-connection times */
    private double reconnectionProgressionFactor = 2.0; // default to [bounded] exponential
    /**
     * Progression factory for geometric series that calculates re-try after failures from message consuming
     */
    private Double consumeErrorProgressionFactor = 2.0; // default to [bounded] exponential
    /**
     * Maximum consumer retries on consume error before sleep.
    */
    private Integer maxConsumeErrorRetryBeforeDelay = 20; // default 20
    /**
     * Number of consume retries upon consume error.
     */
    private int consumerRetryCount = 0;
    /** Upper limit on reconnection attempt duration */
    private long maxReconnectDuration = 1000 * 60 * 1; // 1 min
	/** Reconnect duration in case of a failure */
	private Long reconnectDuration = null; // default null
    /** The JNDI context properties and other general properties */
    private Hashtable<String,String> jmsProperties = new Hashtable<String, String>();
    /** The JNDI Context acuired */
    private Context context = null;
    /** The ConnectionFactory to be used */
    private ConnectionFactory conFactory = null;
    /** The JMS Destination */
    private Destination destination = null;

    /** The list of active tasks thats managed by this instance */
    private final List<MessageListenerTask> pollingTasks =
        Collections.synchronizedList(new ArrayList<MessageListenerTask>());
    /** The per-service JMS message receiver to be invoked after receipt of messages */
    private JMSMessageReceiver jmsMessageReceiver = null;

    /** State of this Task Manager */
    private volatile int serviceTaskManagerState = STATE_STOPPED;
    /** Number of invoker tasks active */
    private AtomicInteger  activeTaskCount = new AtomicInteger(0);
    /** The number of existing JMS message consumers. */
    private final AtomicInteger consumerCount = new AtomicInteger();
    /** The shared thread pool from the Listener */
    private WorkerPool workerPool = null;

    /** The JMS Connection shared between multiple polling tasks - when enabled (reccomended) */
    private Connection sharedConnection = null;

    private String durableSubscriberClientId = null;

    private volatile boolean isOnExceptionError = false;

    private volatile boolean isInitalizeFailed = false;

    /** Is a shard topic subscription (JMS 2.0 feature) **/
    private boolean sharedSubscription = false;

     /**
     * Start or re-start the Task Manager by shutting down any existing worker tasks and
     * re-creating them. However, if this is STM is PAUSED, a start request is ignored.
     * This applies for any connection failures during paused state as well, which then will
     * not try to auto recover
     */
    public synchronized void start() {

        if (serviceTaskManagerState == STATE_PAUSED) {
            log.info("Attempt to re-start paused TaskManager is ignored. Please use resume instead");
            return;
        }

        // if any tasks are running, stop whats running now
        if (!pollingTasks.isEmpty()) {
            stop();
        }

        if (cacheLevel == JMSConstants.CACHE_AUTO) {
            cacheLevel = 
                transactionality == BaseConstants.TRANSACTION_NONE ?
                    JMSConstants.CACHE_CONSUMER : JMSConstants.CACHE_NONE;
        }
        switch (cacheLevel) {
            case JMSConstants.CACHE_NONE:
                log.debug("No JMS resources will be cached/shared between poller " +
                    "worker tasks of service : " + serviceName);
                break;
            case JMSConstants.CACHE_CONNECTION:
                log.debug("Only the JMS Connection will be cached and shared between *all* " +
                    "poller task invocations");
                break;
            case JMSConstants.CACHE_SESSION:
                log.debug("The JMS Connection and Session will be cached and shared between " +
                    "successive poller task invocations");
                break;
            case JMSConstants.CACHE_CONSUMER:
                log.debug("The JMS Connection, Session and MessageConsumer will be cached and " +
                    "shared between successive poller task invocations");
                break;
            default : {
                handleException("Invalid cache level : " + cacheLevel +
                    " for service : " + serviceName);
            }
        }

        for (int i=0; i<concurrentConsumers; i++) {
            if (jmsMessageReceiver.getJmsListener().getState() == BaseConstants.PAUSED) {
                workerPool.execute(new MessageListenerTask(BaseConstants.PAUSED));
            } else {
                workerPool.execute(new MessageListenerTask(BaseConstants.STARTED));
            }
        }

        serviceTaskManagerState = STATE_STARTED;
        log.info("Task manager for service : " + serviceName + " [re-]initialized");

    }

    /**
     * Shutdown the tasks and release any shared resources
     */
    public synchronized void stop() {

        if (log.isDebugEnabled()) {
            log.debug("Stopping ServiceTaskManager for service : " + serviceName);
        }

        if (serviceTaskManagerState != STATE_FAILURE) {
            serviceTaskManagerState = STATE_SHUTTING_DOWN;
        }

        synchronized(pollingTasks) {
            for (MessageListenerTask lstTask : pollingTasks) {
                lstTask.requestShutdown();
            }
        }

        // try to wait a bit for task shutdown
        for (int i=0; i<5; i++) {
            if (activeTaskCount.get() == 0) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignore) {}
        }

        if (sharedConnection != null) {
            try {
                sharedConnection.close();
            } catch (JMSException e) {
                logError("Error closing shared Connection", e);
            } finally {
                sharedConnection = null;
            }
        }

        if (activeTaskCount.get() > 0) {
            log.warn("Unable to shutdown all polling tasks of service : " + serviceName);
        }

        if (serviceTaskManagerState != STATE_FAILURE) {
            serviceTaskManagerState = STATE_STOPPED;
        }
        log.info("Task manager for service : " + serviceName + " shutdown");
    }

    /**
     * Temporarily suspend receipt and processing of messages. Accomplished by stopping the
     * connection / or connections used by the poller tasks
     */
    public synchronized void pause() {
        synchronized (pollingTasks) {
            for (MessageListenerTask lstTask : pollingTasks) {
                lstTask.pause();
            }
        }
        if (sharedConnection != null) {
            try {
                sharedConnection.stop();
            } catch (JMSException e) {
                logError("Error pausing shared Connection", e);
            }
        }
    }

    /**
     * Resume receipt and processing of messages of paused tasks
     */
    public synchronized void resume() {
        synchronized (pollingTasks) {
            for (MessageListenerTask lstTask : pollingTasks) {
                lstTask.resume();
            }
        }
        if (sharedConnection != null) {
            try {
                sharedConnection.start();
            } catch (JMSException e) {
                logError("Error resuming shared Connection", e);
            }
        }
    }

    /**
     * Start a new MessageListenerTask if we are still active, the threshold is not reached, and w
     * e do not have any idle tasks - i.e. scale up listening
     */
    private void scheduleNewTaskIfAppropriate() {
        if (serviceTaskManagerState == STATE_STARTED &&
            pollingTasks.size() < getMaxConcurrentConsumers() && getIdleTaskCount() == 0) {

            if (jmsMessageReceiver.getJmsListener().getState() == BaseConstants.PAUSED) {
                workerPool.execute(new MessageListenerTask(BaseConstants.PAUSED));
            } else {
                workerPool.execute(new MessageListenerTask(BaseConstants.STARTED));
            }
        }
    }

    /**
     * Get the number of MessageListenerTasks that are currently idle
     * @return idle task count
     */
    private int getIdleTaskCount() {
        int count = 0;
        synchronized (pollingTasks) {
            for (MessageListenerTask lstTask : pollingTasks) {
                if (lstTask.isTaskIdle()) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Get the number of MessageListenerTasks that are currently connected to the JMS provider
     * @return connected task count
     */
    private int getConnectedTaskCount() {
        int count = 0;
        synchronized (pollingTasks) {
            for (MessageListenerTask lstTask : pollingTasks) {
                if (lstTask.isConnected()) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * The actual threads/tasks that perform message polling
     */
    private class MessageListenerTask implements Runnable, ExceptionListener {

        /** The Connection used by the polling task */
        private Connection connection = null;
        /** The Sesson used by the polling task */
        private Session session = null;
        /** The MessageConsumer used by the polling task */
        private MessageConsumer consumer = null;
        /** State of the worker polling task */
        private volatile int workerState = STATE_STOPPED;
        /** The number of idle (i.e. without fetching a message) polls for this task */
        private int idleExecutionCount = 0;
        /** Is this task idle right now? */
        private volatile boolean idle = false;
        /** Is this task connected to the JMS provider successfully? */
        private volatile boolean connected = false;
        private volatile boolean listenerPaused = false;

        private boolean connectionReceivedError = false;

        /** As soon as we create a new polling task, add it to the STM for control later */
        MessageListenerTask(int listenerState) {
            synchronized(pollingTasks) {
                pollingTasks.add(this);
                if (listenerState == BaseConstants.PAUSED) {
                    listenerPaused = true;
                }
            }
        }

        /**
         * Pause this polling worker task
         */
        public void pause() {
            if (isActive()) {
                if (connection != null && cacheLevel < JMSConstants.CACHE_CONNECTION) {
                    try {
                        connection.stop();
                    } catch (JMSException e) {
                        log.warn("Error pausing Message Listener task for service : " + serviceName);
                    }
                }
                workerState = STATE_PAUSED;
                listenerPaused = true;
            }
        }

        /**
         * Resume this polling task
         */
        public void resume() {
            if (connection != null && cacheLevel < JMSConstants.CACHE_CONNECTION) {
                try {
                    connection.start();
                } catch (JMSException e) {
                    log.warn("Error resuming Message Listener task for service : " + serviceName);
                }
            }
            workerState = STATE_STARTED;
            listenerPaused = false;
        }

        /**
         * Execute the polling worker task
         */
        public void run() {
            workerState = STATE_STARTED;
            activeTaskCount.getAndIncrement();
            int messageCount = 0;
            long retryDurationOnConsumerFailure = consumeErrorRetryDelay;

            if (log.isDebugEnabled()) {
                log.debug("New poll task starting : thread id = " + Thread.currentThread().getId());
            }

            if (listenerPaused) {
                workerState = STATE_PAUSED;
            }

            try {
                while (isActive() &&
                    (getMaxMessagesPerTask() < 0 || messageCount < getMaxMessagesPerTask()) &&
                    (getConcurrentConsumers() == 1 || idleExecutionCount < getIdleTaskExecutionLimit())) {

                    UserTransaction ut = null;
                    try {
                        if (transactionality == BaseConstants.TRANSACTION_JTA) {
                            ut = getUserTransaction();
                            // We will only create a new tx if there is no tx alive 
                            if (ut.getStatus() == Status.STATUS_NO_TRANSACTION) {
                                ut.begin();
                            }
                        }
                    } catch (NotSupportedException e) {
                        handleException("Listener Task is already associated with a transaction", e);
                    } catch (SystemException e) {
                        handleException("Error starting a JTA transaction", e);
                    }

                    // Get a message by polling, or receive null
                    Message message = receiveMessage();

                    if (log.isTraceEnabled()) {
                        if (message != null) {
                            try {
                                log.trace("<<<<<<< READ message with Message ID : " +
                                    message.getJMSMessageID() + " from : " + destination +
                                    " by Thread ID : " + Thread.currentThread().getId());
                            } catch (JMSException ignore) {}
                        } else {
                            log.trace("No message received by Thread ID : " +
                                Thread.currentThread().getId() + " for destination : " + destination);
                        }
                    }

                    if (connectionReceivedError && (maxConsumeErrorRetryBeforeDelay < consumerRetryCount)) {
                        try {
                            Thread.sleep(retryDurationOnConsumerFailure);
                        } catch (InterruptedException ignore) {
                        }

                        retryDurationOnConsumerFailure = (long) (retryDurationOnConsumerFailure * consumeErrorProgressionFactor);
                        log.info("Error while consuming message from service : " +
                                serviceName + ". Next retry in " + (retryDurationOnConsumerFailure / 1000) +
                                " seconds");
                    } else {
                        retryDurationOnConsumerFailure = consumeErrorRetryDelay;
                    }

                    if (message != null) {
                        idle = false;
                        idleExecutionCount = 0;
                        messageCount++;
                        // I will be busy now while processing this message, so start another if needed
                        scheduleNewTaskIfAppropriate();
                        handleMessage(message, ut);

                    } else {
                        idle = true;
                        idleExecutionCount++;
                    }
                }
			} catch (AxisJMSException e) {
				log.error("Error reciving the message.");
            } finally {
                
                if (log.isTraceEnabled()) {
                    log.trace("Listener task with Thread ID : " + Thread.currentThread().getId() +
                        " is stopping after processing : " + messageCount + " messages :: " +
                        " isActive : " + isActive() + " maxMessagesPerTask : " +
                        getMaxMessagesPerTask() + " concurrentConsumers : " + getConcurrentConsumers() +
                        " idleExecutionCount : " + idleExecutionCount + " idleTaskExecutionLimit : " + 
                        getIdleTaskExecutionLimit());
                } else if (log.isDebugEnabled()) {
                    log.debug("Listener task with Thread ID : " + Thread.currentThread().getId() +
                        " is stopping after processing : " + messageCount + " messages");
                }
                
                // Close the consumer and session before decrementing activeTaskCount.
                // (If we have a shared connection, Qpid deadlocks if the shared connection
                //  is closed on another thread while closing the session)
                closeConsumer(true);
                closeSession(true);
                closeConnection();
                
                workerState = STATE_STOPPED;
                activeTaskCount.getAndDecrement();
                synchronized(pollingTasks) {
                    pollingTasks.remove(this);
                }

                // if this is a JMS onException, ServiceTaskManager#onException will schedule
                // a new polling task
                if (!isOnExceptionError || connectionReceivedError) {
                    if (isInitalizeFailed) {
                        if (reconnectDuration != null) {
                            try {
                                Thread.sleep(reconnectDuration);
                            } catch (InterruptedException ignore) {
                            }
                            log.info("Retry in " + (reconnectDuration / 1000) + " Seconds.");
                        }
                    }
                    // My time is up, so if I am going away, create another
                    scheduleNewTaskIfAppropriate();
                }
            }

        }

        /**
         * Poll for and return a message if available
         *
         * @return a message read, or null
         */
        private Message receiveMessage() {
            connectionReceivedError  = false;

            // get a new connection, session and consumer to prevent a conflict.
            // If idle, it means we can re-use what we already have 
            if (consumer == null) {
                connection = getConnection();
                session = getSession();
                consumer = getMessageConsumer();
                if (log.isDebugEnabled()) {
                    log.debug("Preparing a Connection, Session and Consumer to read messages");
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Waiting for a message for service : " + serviceName + " - duration : "
                    + (getReceiveTimeout() < 0 ? "unlimited" : (getReceiveTimeout() + "ms")));
            }

            try {
                if (getReceiveTimeout() < 0) {
                    Message msg =  consumer.receive();
                    consumerRetryCount = 0;
                    return msg;
                } else {
                    Message msg = consumer.receive(getReceiveTimeout());
                    consumerRetryCount = 0;
                    return msg;
                }
            } catch (IllegalStateException ignore) {
                // probably the consumer (shared) was closed.. which is still ok.. as we didn't read
            } catch (JMSException e) {
                connectionReceivedError  = true;
                consumerRetryCount++;
                if (consumerRetryCount <= maxConsumeErrorRetryBeforeDelay) {
                    log.warn("Could not consume message from: " +
                            serviceName + ". Expect " + (maxConsumeErrorRetryBeforeDelay - consumerRetryCount) + " more " +
                            "retries before exponential sleep!");
                }
                logError("Error receiving message for service : " + serviceName, e);
            }
            return null;
        }

        /**
         * Invoke ultimate message handler/listener and ack message and/or
         * commit/rollback transactions
         * @param message the JMS message received
         * @param ut the UserTransaction used to receive this message, or null
         */
        private void handleMessage(Message message, UserTransaction ut) {

            String messageId = null;
            try {
                messageId = message.getJMSMessageID();
            } catch (JMSException ignore) {}

            boolean commitOrAck = true;
            try {
                commitOrAck = jmsMessageReceiver.onMessage(message, ut);

            } finally {

                // if client acknowledgement is selected, and processing requested ACK
                if (commitOrAck && getSessionAckMode() == Session.CLIENT_ACKNOWLEDGE) {
                    try {
                        message.acknowledge();
                        if (log.isDebugEnabled()) {
                            log.debug("Message : " + messageId + " acknowledged");
                        }
                    } catch (JMSException e) {
                        logError("Error acknowledging message : " + messageId, e);
                    }
                }

                // if session was transacted, commit it or rollback
                try {
                    if (session.getTransacted()) {
                        if (commitOrAck) {
                            session.commit();
                            if (log.isDebugEnabled()) {
                                log.debug("Session for message : " + messageId + " committed");
                            }
                        } else {
                            session.rollback();
                            if (log.isDebugEnabled()) {
                                log.debug("Session for message : " + messageId + " rolled back");
                            }
                        }
                    }
                } catch (JMSException e) {
                    logError("Error " + (commitOrAck ? "committing" : "rolling back") +
                        " local session txn for message : " + messageId, e);
                }

                // if a JTA transaction was being used, commit it or rollback
                try {
                    if (ut != null) {
                        if (commitOrAck) {
                            ut.commit();
                            if (log.isDebugEnabled()) {
                                log.debug("JTA txn for message : " + messageId + " committed");
                            }
                        } else {
                            ut.rollback();
                            if (log.isDebugEnabled()) {
                                log.debug("JTA txn for message : " + messageId + " rolled back");
                            }
                        }
                    }
                } catch (Exception e) {
                    logError("Error " + (commitOrAck ? "committing" : "rolling back") +
                        " JTA txn for message : " + messageId + " from the session", e);
                }

                // close the consumer
                closeConsumer(false);

                closeSession(false);
                closeConnection();
            }
        }

        /** Handle JMS Connection exceptions by re-initializing. A single connection failure could
         * cause re-initialization of multiple MessageListenerTasks / Connections
         */
        public void onException(JMSException j) {

            isOnExceptionError = true;

            if (!isSTMActive()) {
                requestShutdown();
                return;
            }

            setConnected(false);

            if (cacheLevel < JMSConstants.CACHE_CONNECTION) {
                // failed Connection was not shared, thus no need to restart the whole STM
                log.warn("JMS Connection failure : " + j.getMessage());
                requestShutdown();
                return;
            }

            // if we failed while active, update state to show failure
            setServiceTaskManagerState(STATE_FAILURE);
            log.error("JMS Connection failed : " + j.getMessage() + " - shutting down worker tasks");

            int r = 1;

            long retryDuration = initialReconnectDuration;
            boolean connected = false;

            do {
                try {
                    log.info("Reconnection attempt : " + r + " for service : " + serviceName);
                    start();
                } catch (Throwable ignore) {}

                finally {


                    for (int i = 0; i < 5; i++) {
                        if (getConnectedTaskCount() == concurrentConsumers) {
                            connected = true;
                            break;
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignore) {
                        }
                    }

                    if (!connected) {
                        if (reconnectDuration != null) {
                            retryDuration = reconnectDuration;
                            log.error("Reconnection attempt : " + (r++) + " for service : " +
                                    serviceName + " failed. Next retry in " + (retryDuration / 1000) +
                                    " seconds. (Fixed Interval)");
                        } else {
                            retryDuration = (long) (retryDuration * reconnectionProgressionFactor);
                            if (retryDuration > maxReconnectDuration) {
                                retryDuration = maxReconnectDuration;
                                log.info("InitialReconnectDuration reached to MaxReconnectDuration.");
                            }
                            log.error("Reconnection attempt : " + (r++) + " for service : " +
                                      serviceName + " failed. Next retry in " + (retryDuration / 1000) +
                                      " seconds");
                        }
                        try {
                            Thread.sleep(retryDuration);
                            if (getConnectedTaskCount() == concurrentConsumers) {
                                connected = true;
                                log.info("Reconnection attempt: " + r + " for service: " + serviceName +
                                        " was successful!");
                            }
                        } catch (InterruptedException ignore) {
                        }
                    } else {
                        isOnExceptionError = false;
                        log.info("Reconnection attempt: " + r + " for service: " + serviceName +
                                " was successful!");
                    }

                }
            } while (!isSTMActive() || !connected);
        }

        protected void requestShutdown() {
            workerState = STATE_SHUTTING_DOWN;
        }

        private boolean isActive() {
            return workerState == STATE_STARTED;
        }

        protected boolean isTaskIdle() {
            return idle;
        }

        public boolean isConnected() {
            return connected;
        }

        public void setConnected(boolean connected) {
            this.connected = connected;
        }

        /**
         * Get a Connection that could/should be used by this task - depends on the cache level to reuse
         * @return the shared Connection if cache level is higher than CACHE_NONE, or a new Connection
         */
        private Connection getConnection() {
            if (cacheLevel < JMSConstants.CACHE_CONNECTION) {
                // Connection is not shared
                if (connection == null) {
                    connection = createConnection();
                    setConnected(true);
                }
                
            } else if (connection == null) {
                // Connection is shared, but may not have been created
                
                synchronized(ServiceTaskManager.this) {
                    if (sharedConnection == null) {
                        sharedConnection = createConnection();
                    }
                }
                connection = sharedConnection;
                setConnected(true);
                
            }
            // else: Connection is shared and is already referenced by this.connection
            
            return connection;
        }

        /**
         * Get a Session that could/should be used by this task - depends on the cache level to reuse
         * @param connection the connection (could be the shared connection) to use to create a Session
         * @return the shared Session if cache level is higher than CACHE_CONNECTION, or a new Session
         * created using the Connection passed, or a new/shared connection
         */
        private Session getSession() {
            if (session == null || cacheLevel < JMSConstants.CACHE_SESSION) {
                session = createSession();
            }
            return session;
        }

        /**
         * Get a MessageConsumer that chould/should be used by this task - depends on the cache
         * level to reuse
         * @param connection option Connection to be used
         * @param session optional Session to be used
         * @return the shared MessageConsumer if cache level is higher than CACHE_SESSION, or a new
         * MessageConsumer possibly using the Connection and Session passed in
         */
        private MessageConsumer getMessageConsumer() {
            if (consumer == null || cacheLevel < JMSConstants.CACHE_CONSUMER) {
                consumer = createConsumer();
            }
            return consumer;
        }

        /**
         * Close the given Connection, hiding exceptions if any which are logged
         * @param connection the Connection to be closed
         */
        private void closeConnection() {
            if (connection != null &&
                cacheLevel < JMSConstants.CACHE_CONNECTION) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Closing non-shared JMS connection for service : " + serviceName);
                    }
                    connection.close();
                } catch (JMSException e) {
                    logError("Error closing JMS connection", e);
                } finally {
                    connection = null;
                }
            }
        }

        /**
         * Close the given Session, hiding exceptions if any which are logged
         * @param session the Session to be closed
         */
        private void closeSession(boolean forced) {
            if (session != null &&
                (cacheLevel < JMSConstants.CACHE_SESSION || forced)) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Closing non-shared JMS session for service : " + serviceName);
                    }
                    session.close();
                } catch (JMSException e) {
                    logError("Error closing JMS session", e);
                } finally {
                    session = null;
                }
            }
        }

        /**
         * Close the given Consumer, hiding exceptions if any which are logged
         * @param consumer the Consumer to be closed
         */
        private void closeConsumer(boolean forced) {
            if (consumer != null &&
                (cacheLevel < JMSConstants.CACHE_CONSUMER || forced)) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Closing non-shared JMS consumer for service : " + serviceName);
                    }
                    consumerCount.decrementAndGet();
                    consumer.close();
                } catch (JMSException e) {
                    logError("Error closing JMS consumer", e);
                } finally {
                    consumer = null;
                }
            }
        }

        /**
         * Create a new Connection for this STM, using JNDI properties and credentials provided
         * @return a new Connection for this STM, using JNDI properties and credentials provided
         */
        private Connection createConnection() {
            try {
                conFactory = JMSUtils.lookup(
                        getInitialContext(jmsProperties), ConnectionFactory.class, getConnFactoryJNDIName());
                log.debug("Connected to the JMS connection factory : " + getConnFactoryJNDIName());
            } catch (NamingException e) {
                handleException("Error looking up connection factory : " + getConnFactoryJNDIName() +
                    " using JNDI properties : " + JMSUtils.maskAxis2ConfigSensitiveParameters(jmsProperties), e);
            }

            Connection connection = null;
            isInitalizeFailed = false;
            try {
                connection = JMSUtils.createConnection(
                    conFactory,
                    jmsProperties.get(JMSConstants.PARAM_JMS_USERNAME),
                    jmsProperties.get(JMSConstants.PARAM_JMS_PASSWORD),
                    getJmsSpec(), isQueue(), isSubscriptionDurable(), durableSubscriberClientId, isShardSubscription());

                connection.setExceptionListener(this);
                connection.start();
                log.debug("JMS Connection for service : " + serviceName + " created and started");
            } catch (JMSException e) {
				isInitalizeFailed = true;
				String msg =
				             "Error acquiring a JMS connection to : " + getConnFactoryJNDIName() +
				                     " using JNDI properties : " + JMSUtils.maskAxis2ConfigSensitiveParameters
                                     (jmsProperties) + ". " +
				                     e.getMessage();
				if (log.isDebugEnabled()) {
					log.error(msg, e);
				} else {
					log.error(msg);
				}
				throw new AxisJMSException(msg, e);
            }
            return connection;
        }

        /**
         * Create a new Session for this STM
         * @param connection the Connection to be used
         * @return a new Session created using the Connection passed in
         */
        private Session createSession() {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Creating a new JMS Session for service : " + serviceName);
                }
                return JMSUtils.createSession(
                    connection, isSessionTransacted(), getSessionAckMode(), getJmsSpec(), isQueue());

            } catch (JMSException e) {
                handleException("Error creating JMS session for service : " + serviceName, e);
            }
            return null;
        }

        /**
         * Create a new MessageConsumer for this STM
         * @param session the Session to be used
         * @return a new MessageConsumer created using the Session passed in
         */
        private MessageConsumer createConsumer() {
        	isInitalizeFailed = false;
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Creating a new JMS MessageConsumer for service : " + serviceName);
                }

                MessageConsumer consumer = JMSUtils.createConsumer(
                    session, getDestination(session), isQueue(),
                    ((isSubscriptionDurable() || isShardSubscription()) && getDurableSubscriberName() != null ?
                        getDurableSubscriberName() : serviceName), getMessageSelector(), isPubSubNoLocal(),
                        isSubscriptionDurable(), getJmsSpec(), isShardSubscription());
                consumerCount.incrementAndGet();
                return consumer;

            } catch (JMSException e) {
				isInitalizeFailed = true;
				String msg =
				             "Error creating JMS consumer for service : " + serviceName + ". " +
				                     e.getMessage();
				if (log.isDebugEnabled()) {
					log.error(msg, e);
				} else {
					log.error(msg);
				}
				throw new AxisJMSException(msg, e);
            }
        }
    }

    /**
     * Get the InitialContext for lookup using the JNDI parameters applicable to the service
     * @return the InitialContext to be used
     * @throws NamingException
     */
    private Context getInitialContext() throws NamingException {
        if (context == null) {
            context = new InitialContext();
        }
        return context;
    }

    // -------------- mundane private methods ----------------

    /**
     * Get the InitialContext for lookup using the JNDI parameters applicable to the service
     *
     * @return the InitialContext to be used
     * @throws NamingException
     */
    private Context getInitialContext(Hashtable jmsProperties) throws NamingException {
        if (context == null) {
            context = new InitialContext(jmsProperties);
        }
        return new InitialContext(jmsProperties);
    }

    /**
     * Return the JMS Destination for the JNDI name of the Destination from the InitialContext
     * @param session which is used to create the destinations if not present and if possible 
     * @return the JMS Destination to which this STM listens for messages
     */
    private Destination getDestination(Session session) {
        if (destination == null) {
            try {
                context = getInitialContext();
                destination = JMSUtils.lookupDestination(context, getDestinationJNDIName(),
                        JMSUtils.getDestinationTypeAsString(destinationType));
                if (log.isDebugEnabled()) {
                    log.debug("JMS Destination with JNDI name : " + getDestinationJNDIName() +
                        " found for service " + serviceName);
                }
            } catch (NamingException e) {
                try {
                    switch (destinationType) {
                        case JMSConstants.QUEUE: {
                            destination = session.createQueue(getDestinationJNDIName());
                            break;
                        }
                        case JMSConstants.TOPIC: {
                            destination = session.createTopic(getDestinationJNDIName());
                            break;
                        }
                        default: {
                            handleException("Error looking up JMS destination : " +
                                getDestinationJNDIName() + " using JNDI properties : " +
                                jmsProperties, e);
                        }
                    }
                } catch (JMSException j) {
                    handleException("Error looking up JMS destination and auto " +
                            "creating JMS destination : " + getDestinationJNDIName() +
                            " using JNDI properties : " + JMSUtils.maskAxis2ConfigSensitiveParameters(jmsProperties),
                            e);
                }
            }
        }
        return destination;
    }


    /**
     * The UserTransaction to be used, looked up from the JNDI
     * @return The UserTransaction to be used, looked up from the JNDI
     */
    private UserTransaction getUserTransaction() {
        if (!cacheUserTransaction) {
            if (log.isDebugEnabled()) {
                log.debug("Acquiring a new UserTransaction for service : " + serviceName);
            }

            try {
                context = getInitialContext();
                return
                    JMSUtils.lookup(context, UserTransaction.class, getUserTransactionJNDIName());
            } catch (NamingException e) {
                handleException("Error looking up UserTransaction : " + getUserTransactionJNDIName() +
                    " using JNDI properties : " + JMSUtils.maskAxis2ConfigSensitiveParameters(jmsProperties), e);
            }
        }
        
        if (sharedUserTransaction == null) {
            try {
                context = getInitialContext();
                sharedUserTransaction =
                    JMSUtils.lookup(context, UserTransaction.class, getUserTransactionJNDIName());
                if (log.isDebugEnabled()) {
                    log.debug("Acquired shared UserTransaction for service : " + serviceName);
                }
            } catch (NamingException e) {
                handleException("Error looking up UserTransaction : " + getUserTransactionJNDIName() +
                    " using JNDI properties : " + JMSUtils.maskAxis2ConfigSensitiveParameters(jmsProperties), e);
            }
        }
        return sharedUserTransaction;
    }

    // -------------------- trivial methods ---------------------
    private boolean isSTMActive() {
        return serviceTaskManagerState == STATE_STARTED;
    }

    /**
     * Is this STM bound to a Queue, Topic or a JMS 1.1 Generic Destination?
     * @return TRUE for a Queue, FALSE for a Topic and NULL for a Generic Destination
     */
    public Boolean isQueue() {
        if (destinationType == JMSConstants.GENERIC) {
            return null;
        } else {
            return destinationType == JMSConstants.QUEUE;   
        }
    }

    private void logError(String msg, Exception e) {
        log.error(msg, e);
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new AxisJMSException(msg, e);
    }

    private void handleException(String msg) {
        log.error(msg);
        throw new AxisJMSException(msg);
    }

    // -------------- getters and setters ------------------
    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getConnFactoryJNDIName() {
        return connFactoryJNDIName;
    }

    public void setConnFactoryJNDIName(String connFactoryJNDIName) {
        this.connFactoryJNDIName = connFactoryJNDIName;
    }

    public String getDestinationJNDIName() {
        return destinationJNDIName;
    }

    public void setDestinationJNDIName(String destinationJNDIName) {
        this.destinationJNDIName = destinationJNDIName;
    }

    public int getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(int destinationType) {
        this.destinationType = destinationType;
    }

    public String getMessageSelector() {
        return messageSelector;
    }

    public void setMessageSelector(String messageSelector) {
        this.messageSelector = messageSelector;
    }

    public int getTransactionality() {
        return transactionality;
    }

    public void setTransactionality(int transactionality) {
        this.transactionality = transactionality;
        sessionTransacted = (transactionality == BaseConstants.TRANSACTION_LOCAL);
    }

    public void setDurableSubscriberClientId(String durableSubscriberClientId) {
        this.durableSubscriberClientId = durableSubscriberClientId;
    }

    public String getDurableSubscriberClientId() {
        return durableSubscriberClientId;
    }

    public boolean isSessionTransacted() {
        return sessionTransacted;
    }

    public void setSessionTransacted(Boolean sessionTransacted) {
        if (sessionTransacted != null) {
            this.sessionTransacted = sessionTransacted;
            // sessionTransacted means local transactions are used, however !sessionTransacted does
            // not mean that JTA is used.
            // do not change the transactionality based on this value, since some of the transaction 
            // providers require JTA to be set and also sessionTransacted = true.
            // if (sessionTransacted) {
            //     transactionality = BaseConstants.TRANSACTION_LOCAL;
            // }
        }
    }

    public int getSessionAckMode() {
        return sessionAckMode;
    }

    public void setSessionAckMode(int sessionAckMode) {
        this.sessionAckMode = sessionAckMode;
    }

    public boolean isSubscriptionDurable() {
        return subscriptionDurable;
    }

    public void setSubscriptionDurable(Boolean subscriptionDurable) {
        if (subscriptionDurable != null) {
            this.subscriptionDurable = subscriptionDurable;
        }
    }

    public String getDurableSubscriberName() {
        return durableSubscriberName;
    }

    public void setDurableSubscriberName(String durableSubscriberName) {
        this.durableSubscriberName = durableSubscriberName;
    }

    public boolean isShardSubscription() {
        return sharedSubscription;
    }

    public void setSharedSubscription(Boolean sharedSubscription) {
        if (sharedSubscription != null) {
            this.sharedSubscription = sharedSubscription;
        }
    }

    public boolean isPubSubNoLocal() {
        return pubSubNoLocal;
    }

    public void setPubSubNoLocal(Boolean pubSubNoLocal) {
        if (pubSubNoLocal != null) {
            this.pubSubNoLocal = pubSubNoLocal;
        }
    }

    public int getConcurrentConsumers() {
        return concurrentConsumers;
    }

    public void setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
    }

    public int getMaxConcurrentConsumers() {
        return maxConcurrentConsumers;
    }

    public void setMaxConcurrentConsumers(int maxConcurrentConsumers) {
        this.maxConcurrentConsumers = maxConcurrentConsumers;
    }

    public int getIdleTaskExecutionLimit() {
        return idleTaskExecutionLimit;
    }

    public void setIdleTaskExecutionLimit(int idleTaskExecutionLimit) {
        this.idleTaskExecutionLimit = idleTaskExecutionLimit;
    }

    public int getReceiveTimeout() {
        return receiveTimeout;
    }

    public void setReceiveTimeout(int receiveTimeout) {
        this.receiveTimeout = receiveTimeout;
    }

    public int getCacheLevel() {
        return cacheLevel;
    }

    public void setCacheLevel(int cacheLevel) {
        this.cacheLevel = cacheLevel;
    }

    public int getInitialReconnectDuration() {
        return initialReconnectDuration;
    }

    public void setInitialReconnectDuration(int initialReconnectDuration) {
        this.initialReconnectDuration = initialReconnectDuration;
    }

    public double getReconnectionProgressionFactor() {
        return reconnectionProgressionFactor;
    }

    public void setReconnectionProgressionFactor(double reconnectionProgressionFactor) {
        this.reconnectionProgressionFactor = reconnectionProgressionFactor;
    }

    public long getMaxReconnectDuration() {
        return maxReconnectDuration;
    }

    public void setMaxReconnectDuration(long maxReconnectDuration) {
        this.maxReconnectDuration = maxReconnectDuration;
    }
    
	public Long getReconnectDuration() {
		return reconnectDuration;
	}

	public void setReconnectDuration(Long reconnectDuration) {
		this.reconnectDuration = reconnectDuration;
	}

    public void setMaxConsumeErrorRetryBeforeDelay(Integer maxConsumeErrorRetryBeforeDelay) {
        if (maxConsumeErrorRetryBeforeDelay != null) {
            this.maxConsumeErrorRetryBeforeDelay = maxConsumeErrorRetryBeforeDelay;
        }
    }

    public void setConsumeErrorProgressionFactor(Double consumeErrorProgressionFactor) {
        if (consumeErrorProgressionFactor != null) {
            this.consumeErrorProgressionFactor = consumeErrorProgressionFactor;
        }
    }

    public void setConsumeErrorRetryDelay(Integer consumeErrorRetryDelay) {
        if (consumeErrorRetryDelay != null) {
            this.consumeErrorRetryDelay = consumeErrorRetryDelay;
        }
    }

    public int getMaxMessagesPerTask() {
        return maxMessagesPerTask;
    }

    public void setMaxMessagesPerTask(int maxMessagesPerTask) {
        this.maxMessagesPerTask = maxMessagesPerTask;
    }

    public String getUserTransactionJNDIName() {
        return userTransactionJNDIName;
    }

    public void setUserTransactionJNDIName(String userTransactionJNDIName) {
        if (userTransactionJNDIName != null) {
            this.userTransactionJNDIName = userTransactionJNDIName;
        }
    }

    public boolean isCacheUserTransaction() {
        return cacheUserTransaction;
    }

    public void setCacheUserTransaction(Boolean cacheUserTransaction) {
        if (cacheUserTransaction != null) {
            this.cacheUserTransaction = cacheUserTransaction;
        }
    }

    public String getJmsSpec() {
        return jmsSpec;
    }

    public void setJmsSpec(String jmsSpec) {
        this.jmsSpec = jmsSpec;
    }

    public Hashtable<String, String> getJmsProperties() {
        return jmsProperties;
    }

    public void addJmsProperties(Map<String, String> jmsProperties) {
        this.jmsProperties.putAll(jmsProperties);
    }

    public void removeJmsProperties(String key) {
        this.jmsProperties.remove(key);
    }

    public Context getContext() {
        return context;
    }

    public ConnectionFactory getConnectionFactory() {
        return conFactory;
    }

    public List<MessageListenerTask> getPollingTasks() {
        return pollingTasks;
    }

    public void setJmsMessageReceiver(JMSMessageReceiver jmsMessageReceiver) {
        this.jmsMessageReceiver = jmsMessageReceiver;
    }

    public void setWorkerPool(WorkerPool workerPool) {
        this.workerPool = workerPool;
    }

    public int getActiveTaskCount() {
        return activeTaskCount.get();
    }
    
    /**
     * Get the number of existing JMS message consumers.
     * 
     * @return the number of consumers
     */
    public int getConsumerCount() {
        return consumerCount.get();
    }
    
    public void setServiceTaskManagerState(int serviceTaskManagerState) {
        this.serviceTaskManagerState = serviceTaskManagerState;
    }
}

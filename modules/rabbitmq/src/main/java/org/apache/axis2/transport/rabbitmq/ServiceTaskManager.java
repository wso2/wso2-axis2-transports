/*
* Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.axiom.om.OMException;
import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.axis2.transport.rabbitmq.utils.AxisRabbitMQException;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQConstants;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Each service will have one ServiceTaskManager instance that will send, manage and also destroy
 * idle tasks created for it, for message receipt. It uses the MessageListenerTask to poll for the
 * RabbitMQ AMQP Listening destination and consume messages. The consumed messages is build and sent to
 * axis2 engine for processing
 */
public class ServiceTaskManager {
    private static final Log log = LogFactory.getLog(ServiceTaskManager.class);

    private static final int STATE_STOPPED = 0;
    private static final int STATE_STARTING = 1;
    private static final int STATE_STARTED = 2;
    private static final int STATE_PAUSED = 3;
    private static final int STATE_SHUTTING_DOWN = 4;
    private static final int STATE_FAULTY = 5;
    private static final int STATE_DUMMY = -1;

    /**
     * Number of concurrent consumers - for PubSub, this should be 1 to prevent multiple receipt
     */
    private int concurrentConsumers = RabbitMQConstants.CONCURRENT_CONSUMER_COUNT_DEFAULT;

    /**
     * The JMS Connection shared between multiple polling tasks - when enabled (reccomended)
     */
    private volatile Connection sharedConnection = null;

    /**
     * JMS Resource cache level - Connection, Session, Consumer. Auto will select safe default
     */
    private volatile int cacheLevel = RabbitMQConstants.CACHE_NONE;

    private WorkerPool workerPool = null;

    private volatile String serviceName;

    private volatile Hashtable<String, String> rabbitMQProperties = new Hashtable<>();

    private final RabbitMQConnectionFactory rabbitMQConnectionFactory;

    private final List<MessageListenerTask> pollingTasks =
            Collections.synchronizedList(new ArrayList<MessageListenerTask>());
    private volatile RabbitMQMessageReceiver rabbitMQMessageReceiver;
    private State serviceTaskManagerState;

    /**
     * Class to keep States of the ServiceTaskManager and MessageListner
     */
    private class State {
        private volatile int state = STATE_STOPPED;

        public State(boolean update, int state) {
            getUpdateState(update, state);
        }

        /**
         * This method will get or update the states in thread safe manner
         *
         * @param update
         * @param state
         * @return state
         */
        public int getUpdateState(boolean update, int state) {
            synchronized (this) {
                if (update) {
                    this.state = state;
                }
                return this.state;
            }
        }

        /**
         * This method is to get state
         *
         * @return state
         */
        public int getState() {
            return this.state;
        }

        /**
         * This method is to check whether the state is active or not
         *
         * @return true if active and false otherwise
         */
        public boolean isActive() {
            if (this.state == STATE_STARTED) {
                return true;
            }
            return false;
        }
    }

    /**
     * Constructor of service task manager.
     *
     * @param rabbitMQConnectionFactory
     */
    public ServiceTaskManager(
            RabbitMQConnectionFactory rabbitMQConnectionFactory) {
        this.rabbitMQConnectionFactory = rabbitMQConnectionFactory;
        this.serviceTaskManagerState = new State(true, STATE_STOPPED);
    }

    /**
     * Start  the Task Manager by adding a new MessageListenerTasks to the worker pool.
     */
    public void start() {
        serviceTaskManagerState.getUpdateState(true, STATE_STARTING);
        //set the concurrentConsumerCount value so that, serviceTask manager will start that number of messagelistners
        String concurrentConsumerCountString = rabbitMQProperties.get(RabbitMQConstants.CONCURRENT_CONSUMER_COUNT);
        if (concurrentConsumerCountString != null && !"".equals(concurrentConsumerCountString)) {
            try {
                concurrentConsumers = Integer.parseInt(concurrentConsumerCountString);
            } catch (NumberFormatException e) {
                concurrentConsumers = RabbitMQConstants.CONCURRENT_CONSUMER_COUNT_DEFAULT;
                log.warn("Can't parse given RabbitMQ concurrentConsumerCount value as a integer, hence using " +
                         "channel with default (one MessageListner), provided concurrentConsumerCount value - " +
                         concurrentConsumerCountString);
            }
        }
        /**
         * set the cacheLevel value so that, message listners may or may not share same connection depending on this
         * value, 0 means no cache (connection per listner) and 1 means shared connection (connection shared between
         * listners)
         */
        String cacheLevelString = rabbitMQProperties.get(RabbitMQConstants.CACHE_LEVEL);
        if (cacheLevelString != null && !"".equals(cacheLevelString)) {
            try {
                cacheLevel = Integer.parseInt(cacheLevelString);
            } catch (NumberFormatException e) {
                cacheLevel = RabbitMQConstants.CACHE_NONE;
                log.warn("Can't parse given RabbitMQ cacheLevel value as a integer, hence using " +
                         "default (CACHE_NONE - no cache), provided cacheLevel value - " + cacheLevelString);
            }
        }
        for (int i = 0; i < concurrentConsumers; i++) {
            workerPool.execute(new MessageListenerTask());
        }
        serviceTaskManagerState.getUpdateState(true, STATE_STARTED);
    }

    /**
     * Stop the consumer by changing the state, later closing the connections.
     */
    public void stop() {
        serviceTaskManagerState.getUpdateState(true, STATE_SHUTTING_DOWN);
        synchronized (pollingTasks) {
            for (MessageListenerTask lstTask : pollingTasks) {
                lstTask.requestShutdown();
            }
            try {
                //Waiting DEFAULT_WAIT_TIME_BEFORE_CLOSING before starting to close connections
                Thread.sleep(RabbitMQConstants.DEFAULT_WAIT_TIME_BEFORE_CLOSING);
            } catch (InterruptedException e) {
                log.warn("Closing connections before waiting for them to be closed automatically, may throw " +
                         "exceptions " + serviceName, e);
            }
            if (cacheLevel >= RabbitMQConstants.CACHE_CONNECTION) {
                closeSharedConnection();
            } else {
                for (MessageListenerTask lstTask : pollingTasks) {
                    lstTask.closeConnection();
                }
            }
        }
        serviceTaskManagerState.getUpdateState(true, STATE_STOPPED);
    }

    /**
     * Helper method to close the shared connection(if connection caching is enabled)
     */
    private void closeSharedConnection() {
        if (sharedConnection != null) {
            try {
                if (sharedConnection.isOpen()) {
                    sharedConnection.close();
                    log.info("RabbitMQ sharedConnection closed for service " + serviceName);
                }
            } catch (AlreadyClosedException e) {
                log.warn("Error while closing sharedConnection, AlreadyClosedException, service - " + serviceName +
                         ", Listner id - " + Thread.currentThread().getId());
                if (log.isDebugEnabled()) {
                    log.debug("Error while closing sharedConnection, AlreadyClosedException, service - " +
                              serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                }
            } catch (ShutdownSignalException ex) {
                log.warn("Error while closing sharedConnection, ShutdownSignalException, service - " + serviceName +
                         ", Listner id - " + Thread.currentThread().getId());
                if (log.isDebugEnabled()) {
                    log.debug("Error while closing sharedConnection, ShutdownSignalException, service - " +
                              serviceName + ", Listner id - " + Thread.currentThread().getId(), ex);
                }
            } catch (SocketException exx) {
                log.warn("Error while closing sharedConnection, SocketException, service - " + serviceName +
                         ", Listner id - " + Thread.currentThread().getId());
                if (log.isDebugEnabled()) {
                    log.debug("Error while closing sharedConnection, SocketException, service - " +
                              serviceName + ", Listner id - " + Thread.currentThread().getId(), exx);
                }
            } catch (IOException e) {
                log.warn("Error while closing sharedConnection, IOException, service - " + serviceName +
                         ", Listner id - " + Thread.currentThread().getId());
                if (log.isDebugEnabled()) {
                    log.debug("Error while closing sharedConnection, IOException, service - " +
                              serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                }
            } finally {
                sharedConnection = null;
            }
        }
    }

    @Deprecated
    public synchronized void pause() {
        //TODO implement me ..
    }

    @Deprecated
    public synchronized void resume() {
        //TODO implement me ..
    }

    public void setWorkerPool(WorkerPool workerPool) {
        this.workerPool = workerPool;
    }

    public void setRabbitMQMessageReceiver(RabbitMQMessageReceiver rabbitMQMessageReceiver) {
        this.rabbitMQMessageReceiver = rabbitMQMessageReceiver;
    }

    public Hashtable<String, String> getRabbitMQProperties() {
        return rabbitMQProperties;
    }

    public void addRabbitMQProperties(Map<String, String> rabbitMQProperties) {
        this.rabbitMQProperties.putAll(rabbitMQProperties);
    }

    public void removeAMQPProperties(String key) {
        this.rabbitMQProperties.remove(key);
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * The actual threads/tasks that perform message polling
     */
    private class MessageListenerTask implements Runnable {
        private volatile int listnerState;
        private String queueName, routeKey, exchangeName;
        private boolean autoAck = false;
        private Connection localConnection = null;
        private int qos = -1; //-1 means qos is not specified, in that case only basic qos will be applied to channel
        private QueueingConsumer queueingConsumer;
        private volatile boolean connected = false;

        MessageListenerTask() {
            synchronized (pollingTasks) {
                getUpdateListnerState(true, STATE_STOPPED);
                pollingTasks.add(this);
            }
        }

        @Deprecated
        public void pause() {
            //TODO implement me
        }

        @Deprecated
        public void resume() {
            //TODO implement me
        }

        /**
         * Execute the polling worker task
         * Actual reconnection is not happening in here, re-connection is handled by  the rabbitmq connection
         * itself. The only use of recovery interval in here is that to output the info message
         */
        public void run() {
            try {
                getUpdateListnerState(true, STATE_STARTED);
                while (isListnerActive()) {
                    try {
                        initConsumer();
                        startConsumer();
                    } catch (AlreadyClosedException e) {
                        if (isServiceTaskManagerActive()) {
                            log.error("Error, Connection already closed " + serviceName + ", Listner id - " +
                                      Thread.currentThread().getId(), e);
                            if (!retryIfNotStopped()) {
                                break;
                            }
                        } else {
                            log.warn("Error, Connection already closed " + serviceName + ", Listner id - " +
                                     Thread.currentThread().getId());
                            if (log.isDebugEnabled()) {
                                log.debug("Error, Connection already closed " + serviceName + ", Listner id - " +
                                          Thread.currentThread().getId(), e);
                            }
                        }
                    } catch (ShutdownSignalException sse) {
                        if (sse.isInitiatedByApplication()) {
                            log.warn("RabbitMQ Listener of the service " + serviceName + " was disconnected, " +
                                     "Shutdown signal issued, Listner id - " + Thread.currentThread().getId());
                            if (log.isDebugEnabled()) {
                                log.debug("RabbitMQ Listener of the service " + serviceName + " was disconnected, " +
                                          "Shutdown signal issued, Listner id - " + Thread.currentThread().getId(), sse);
                            }
                            getUpdateListnerState(true, STATE_STOPPED);
                            break;
                        } else if (!isServiceTaskManagerActive()) {
                            log.warn("RabbitMQ Listener of the service " + serviceName +
                                     " was disconnected, Listner id - " + Thread.currentThread().getId());
                            if (log.isDebugEnabled()) {
                                log.debug("RabbitMQ Listener of the service " + serviceName +
                                          " was disconnected, Listner id - " + Thread.currentThread().getId(), sse);
                            }
                        } else {
                            log.error("RabbitMQ Listener of the service " + serviceName + " was disconnected, " +
                                      "task manager still active, Listner id - " + Thread.currentThread().getId(), sse);
                            if (!retryIfNotStopped()) {
                                break;
                            }
                        }
                    } catch (OMException e) {
                        log.error("Invalid Message Format while Consuming the message, Listner id - " +
                                  Thread.currentThread().getId(), e);
                    } catch (SocketException exx) {
                        if (!isServiceTaskManagerActive()) {
                            log.warn("RabbitMQ listner disconnected, service - " + serviceName +
                                     ", Listner id - " + Thread.currentThread().getId());
                            if (log.isDebugEnabled()) {
                                log.debug("RabbitMQ listner disconnected, service - " + serviceName +
                                          ", Listner id - " + Thread.currentThread().getId(), exx);
                            }
                        } else {
                            log.error("RabbitMQ listner disconnected, service - " + serviceName +
                                      ", Listner id - " + Thread.currentThread().getId(), exx);
                        }
                    } catch (IOException e) {
                        if (e.getCause() instanceof ShutdownSignalException &&
                            ((ShutdownSignalException) e.getCause()).isInitiatedByApplication()) {
                            log.warn("RabbitMQ Listener of the service " + serviceName + " was disconnected, " +
                                     "Shutdown signal issued, Listner id - " + Thread.currentThread().getId());
                            if (log.isDebugEnabled()) {
                                log.debug("RabbitMQ Listener of the service " + serviceName + " was disconnected, " +
                                          "Shutdown signal issued, Listner id - " + Thread.currentThread().getId(), e);
                            }
                            getUpdateListnerState(true, STATE_STOPPED);
                            break;
                        } else if (!isServiceTaskManagerActive()) {
                            log.warn("RabbitMQ Listener of the service " + serviceName + " was disconnected, " +
                                     "IOException occurred, Listner id - " + Thread.currentThread().getId());
                            if (log.isDebugEnabled()) {
                                log.debug("RabbitMQ Listener of the service " + serviceName + " was disconnected, " +
                                          "IOException occurred, Listner id - " + Thread.currentThread().getId(), e);
                            }
                        } else {
                            log.error("RabbitMQ Listener of the service " + serviceName + " was disconnected, " +
                                      "IOException occurred, but task manager still active, Listner id  " +
                                      Thread.currentThread().getId(), e);
                            if (!retryIfNotStopped()) {
                                break;
                            }
                        }
                    } catch (AxisRabbitMQException e) {
                        if (isServiceTaskManagerActive()) {
                            log.error("Error, Connection closed, AxisRabbitMQException occurred, service " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                            if (!retryIfNotStopped()) {
                                break;
                            }
                        } else {
                            log.warn("Error, Connection closed, AxisRabbitMQException occurred, service " +
                                     serviceName + ", Listner id - " + Thread.currentThread().getId());
                            if (log.isDebugEnabled()) {
                                log.debug("Error, Connection closed, AxisRabbitMQException occurred, service " +
                                          serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                            }
                        }
                    } catch (Exception e) {
                        if (isServiceTaskManagerActive()) {
                            log.error("Error, Connection closed, Exception occurred, service " + serviceName +
                                      ", Listner id - " + Thread.currentThread().getId(), e);
                            if (!retryIfNotStopped()) {
                                break;
                            }
                        } else {
                            log.warn("Error, Connection closed, Exception occurred, service " + serviceName +
                                     ", Listner id - " + Thread.currentThread().getId());
                            if (log.isDebugEnabled()) {
                                log.debug("Error, Connection closed, Exception occurred, service " + serviceName +
                                          ", Listner id - " + Thread.currentThread().getId(), e);
                            }
                        }
                    }

                }
            } finally {
                closeConnection();
                getUpdateListnerState(true, STATE_STOPPED);
                synchronized (pollingTasks) {
                    pollingTasks.remove(this);
                }
            }
        }

        /**
         * Create a queue consumer using the properties form transport listener configuration.
         * When connection is re-connected. This method will request for the connection and create
         * channel, queues, exchanges and bind queues to exchanges before consuming messages
         *
         * @throws IOException on error
         */
        private void initConsumer() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Initializing consumer for service " + serviceName);
            }
            //set the qos value for the RMQ channel so it will be applied when getting a channel back
            String qosString = rabbitMQProperties.get(RabbitMQConstants.CONSUMER_QOS);
            if (qosString != null && !"".equals(qosString)) {
                try {
                    qos = Integer.parseInt(qosString);
                } catch (NumberFormatException e) {
                    qos = -1;
                    log.warn("Can't parse given RabbitMQ qos value as a integer, hence using " +
                             "channel without qos, provided qos value - " + qosString);
                }
            }
            queueingConsumer = new QueueingConsumer(getChannel());
            queueName = rabbitMQProperties.get(RabbitMQConstants.QUEUE_NAME);
            routeKey = rabbitMQProperties.get(RabbitMQConstants.QUEUE_ROUTING_KEY);
            exchangeName = rabbitMQProperties.get(RabbitMQConstants.EXCHANGE_NAME);

            if (log.isDebugEnabled()) {
                log.debug("Starting MessageListner Thread - " + Thread.currentThread().getId() +
                          ", with channel(hashcode) - " + queueingConsumer.getChannel().hashCode());
            }
            String autoAckStringValue = rabbitMQProperties.get(RabbitMQConstants.QUEUE_AUTO_ACK);
            if (autoAckStringValue != null) {
                try {
                    autoAck = Boolean.parseBoolean(autoAckStringValue);
                } catch (Exception e) {
                    log.debug("Format error in rabbitmq.queue.auto.ack parameter");
                }
            }
            //If no queue name is specified then service name will be used as queue name
            if (StringUtils.isEmpty(queueName)) {
                queueName = serviceName;
                log.info("No queue name is specified for " + serviceName + ". " +
                         "Service name will be used as queue name");
            }

            if (routeKey == null) {
                log.info(
                        "No routing key specified. Using queue name as the " +
                        "routing key.");
                routeKey = queueName;
            }

            String queueAutoDeclareStr = rabbitMQProperties.get(RabbitMQConstants.QUEUE_AUTODECLARE);
            String exchangeAutoDeclareStr = rabbitMQProperties.get(RabbitMQConstants.EXCHANGE_AUTODECLARE);

            boolean queueAutoDeclare = true;
            boolean exchangeAutoDeclare = true;

            if (!StringUtils.isEmpty(queueAutoDeclareStr)) {
                queueAutoDeclare = Boolean.parseBoolean(queueAutoDeclareStr);
            }

            if (!StringUtils.isEmpty(exchangeAutoDeclareStr)) {
                exchangeAutoDeclare = Boolean.parseBoolean(exchangeAutoDeclareStr);
            }
            if (queueAutoDeclare && !StringUtils.isEmpty(queueName)) {
                //declaring queue
                RabbitMQUtils.declareQueue(queueingConsumer.getChannel(), queueName, rabbitMQProperties);
            }

            if (exchangeAutoDeclare && !StringUtils.isEmpty(exchangeName)) {
                //declaring exchange
                RabbitMQUtils.declareExchange(queueingConsumer.getChannel(), exchangeName, rabbitMQProperties);
                queueingConsumer.getChannel().queueBind(queueName, exchangeName, routeKey);
                if (log.isDebugEnabled()) {
                    log.debug("Bind queue '" + queueName + "' to exchange '" + exchangeName + "' with route key '" +
                              routeKey + "'");
                }
            }

            String consumerTagString = rabbitMQProperties.get(RabbitMQConstants.CONSUMER_TAG);
            if (consumerTagString != null) {
                queueingConsumer.getChannel().basicConsume(queueName, autoAck, consumerTagString, queueingConsumer);
                if (log.isDebugEnabled()) {
                    log.debug("Start consuming queue '" + queueName + "' with consumer tag '" + consumerTagString +
                              "' for service " + serviceName);
                }
            } else {
                consumerTagString = queueingConsumer.getChannel().basicConsume(queueName, autoAck, queueingConsumer);
                if (log.isDebugEnabled()) {
                    log.debug("Start consuming queue '" + queueName + "' with consumer tag '" + consumerTagString +
                              "' for service " + serviceName);
                }
            }
        }

        /**
         * Used to start consuming messages. This method is called in startup when reconnection  happens
         *
         * @throws ShutdownSignalException
         * @throws IOException
         */
        private void startConsumer() throws ShutdownSignalException, IOException {
            //unable to connect to the queue
            if (queueingConsumer == null) {
                getUpdateListnerState(true, STATE_STOPPED);
                return;
            }

            Channel channel = queueingConsumer.getChannel();

            while (isListnerActive()) {
                try {
                    channel.txSelect();
                } catch (SocketException exx) {
                    if (!isServiceTaskManagerActive()) {
                        throw exx;
                    } else {
                        log.error("Error while starting transaction, SocketException, service - " + serviceName +
                                  ", Listner id - " + Thread.currentThread().getId(), exx);
                        continue;
                    }
                } catch (IOException e) {
                    if (!isServiceTaskManagerActive()) {
                        throw e;
                    } else {
                        log.error("Error while starting transaction, IOException, service - " + serviceName +
                                  ", Listner id - " + Thread.currentThread().getId(), e);
                        continue;
                    }
                }

                boolean successful = false;

                RabbitMQMessage message = null;
                try {
                    message = getConsumerDelivery(queueingConsumer);
                } catch (InterruptedException e) {
                    log.error("Error while consuming message", e);
                    continue;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Processing message by Message Listner Thread - " + Thread.currentThread().getId() +
                              ", time - " + System.nanoTime() + ", channel(hashcode) - " + channel.hashCode());
                }

                if (message != null) {
                    try {
                        successful = rabbitMQMessageReceiver.onMessage(message);
                    } finally {
                        if (successful) {
                            try {
                                if (!autoAck) {
                                    channel.basicAck(message.getDeliveryTag(), false);
                                }
                                channel.txCommit();
                            } catch (SocketException exx) {
                                if (!isServiceTaskManagerActive()) {
                                    throw exx;
                                } else {
                                    log.error("Error while committing transaction, SocketException, service - " + serviceName +
                                              ", Listner id - " + Thread.currentThread().getId(), exx);
                                    continue;
                                }
                            } catch (IOException e) {
                                if (!isServiceTaskManagerActive()) {
                                    throw e;
                                } else {
                                    log.error("Error while committing transaction, IOException, service - " + serviceName +
                                              ", Listner id - " + Thread.currentThread().getId(), e);
                                    continue;
                                }
                            }
                        } else {
                            try {
                                channel.txRollback();
                            } catch (SocketException exx) {
                                if (!isServiceTaskManagerActive()) {
                                    throw exx;
                                } else {
                                    log.error("Error while trying to roll back transaction, SocketException, service - " + serviceName +
                                              ", Listner id - " + Thread.currentThread().getId(), exx);
                                    continue;
                                }
                            } catch (IOException e) {
                                if (!isServiceTaskManagerActive()) {
                                    throw e;
                                } else {
                                    log.error("Error while trying to roll back transaction, IOException, service - " + serviceName +
                                              ", Listner id - " + Thread.currentThread().getId(), e);
                                    continue;
                                }
                            }
                        }
                    }
                }
            }

        }

        /**
         * Returns the delivery from the consumer
         *
         * @param consumer the consumer to get the delivery
         * @return RabbitMQMessage consumed by the consumer
         * @throws InterruptedException on error
         * @throws com.rabbitmq.client.ShutdownSignalException
         */
        private RabbitMQMessage getConsumerDelivery(QueueingConsumer consumer)
                throws InterruptedException, ShutdownSignalException {
            RabbitMQMessage message = new RabbitMQMessage();
            QueueingConsumer.Delivery delivery = null;
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Waiting for next delivery from queue for service " + serviceName +
                              ", Listner id - " + Thread.currentThread().getId());
                }
                delivery = consumer.nextDelivery();
            } catch (InterruptedException e) {
                return null;
            } catch (ConsumerCancelledException e) {
                return null;
            }

            if (delivery != null) {
                AMQP.BasicProperties properties = delivery.getProperties();
                Map<String, Object> headers = properties.getHeaders();
                message.setBody(delivery.getBody());
                message.setDeliveryTag(delivery.getEnvelope().getDeliveryTag());
                message.setReplyTo(properties.getReplyTo());
                message.setMessageId(properties.getMessageId());

                // Content type is as set in delivered message. If not, from service parameters.
                String contentType = properties.getContentType();
                if (contentType == null) {
                    contentType = rabbitMQProperties.get(RabbitMQConstants.CONTENT_TYPE);
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
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Queue delivery item is null for service " + serviceName);
                }
                return null;
            }
            return message;
        }

        /**
         * Retrying the connection if service not deactivated
         *
         * @return true if reconnection successful false otherwise
         */
        private boolean retryIfNotStopped() {
            int retryInterval = rabbitMQConnectionFactory.getRetryInterval();
            int retryCountMax = rabbitMQConnectionFactory.getRetryCount();
            int retryCount = 0;
            while (isListnerActive() && !localConnection.isOpen()
                   && ((retryCountMax == -1) || (retryCount < retryCountMax))) {
                retryCount++;
                log.info("Attempting to reconnect to RabbitMQ Broker for the service " + serviceName +
                         " in " + retryInterval + " ms, Listner id - " + Thread.currentThread().getId());
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e) {
                    log.error("Error while trying to reconnect to RabbitMQ Broker for the service - " + serviceName +
                              ", Listner id - " + Thread.currentThread().getId(), e);
                }
            }
            if (!isListnerActive() || !serviceTaskManagerState.isActive()) {
                if (log.isDebugEnabled()) {
                    log.debug("Service deactivated during waiting period, service - " + serviceName +
                              ", Listner id - " + Thread.currentThread().getId());
                }
                getUpdateListnerState(true, STATE_SHUTTING_DOWN);
                return false;
            } else if (localConnection.isOpen()) {
                log.info("Successfully reconnected to RabbitMQ Broker for the service " + serviceName +
                         ", Listner id - " + Thread.currentThread().getId());
                return true;
            }
            log.error("Could not reconnect to the RabbitMQ Broker for the service " + serviceName +
                      ". Connection is closed, Listner id - " + Thread.currentThread().getId());
            getUpdateListnerState(true, STATE_FAULTY);
            return false;

        }

        /**
         * This will be true if there is a rabbitMQ connection
         *
         * @return true if connected, false otherwise
         */
        @Deprecated
        public boolean isConnected() {
            return connected;
        }

        /**
         * Setter method for connected
         *
         * @param connected
         */
        @Deprecated
        public void setConnected(boolean connected) {
            this.connected = connected;
        }

        /**
         * Helper method to get create and get the channel
         *
         * @return channel
         * @throws IOException
         */
        private Channel getChannel() throws IOException {
            Channel channel = getConnection().createChannel();
            //If qos is applicable, then apply qos before returning the channel
            if (this.qos > 0) {
                channel.basicQos(this.qos);
            }
            return channel;
        }

        /**
         * Helper method to get connection. If connection caching is enabled, return shared connection, else create a
         * local connection
         *
         * @return localConnection
         * @throws IOException
         */
        private Connection getConnection() throws IOException {
            if (cacheLevel < RabbitMQConstants.CACHE_CONNECTION) {
                // Connection is not shared
                if (localConnection == null) {
                    if (isServiceTaskManagerActive()) {
                        localConnection = createConnection();
                        setConnected(true);
                    } else {
                        throw new AxisRabbitMQException("Error, Shutdown signal issued, hence won't create " +
                                                        "the localConnection, service - " + serviceName +
                                                        ", Listner id - " + Thread.currentThread().getId());
                    }
                }
            } else if (localConnection == null) {
                // Connection is shared, but may not have been created
                synchronized (ServiceTaskManager.this) {
                    if (sharedConnection == null) {
                        if (isServiceTaskManagerActive()) {
                            sharedConnection = createConnection();
                        } else {
                            throw new AxisRabbitMQException("Error, Shutdown signal issued, hence won't create " +
                                                            "the sharedConnection, service - " + serviceName +
                                                            ", Listner id - " + Thread.currentThread().getId());
                        }
                    }
                }
                localConnection = sharedConnection;
                setConnected(true);
            }
            // else: Connection is shared and is already referenced by this.connection
            return localConnection;
        }

        /**
         * Helper method to create connection using connection factory.
         *
         * @return connection
         * @throws IOException
         */
        private Connection createConnection() throws IOException {
            Connection connection = null;
            try {
                connection = rabbitMQConnectionFactory.createConnection();
                log.info("RabbitMQ connection created for service " + serviceName);
            } catch (Exception e) {
                log.error("Error while creating RabbitMQ connection for service " + serviceName, e);
                throw new AxisRabbitMQException("Error while creating RabbitMQ connection for service " + serviceName, e);
            }
            return connection;
        }

        /**
         * Method used to change states to STATE_SHUTTING_DOWN, so in next cycle, message listner will shut down
         */
        protected void requestShutdown() {
            getUpdateListnerState(true, STATE_SHUTTING_DOWN);
        }

        /**
         * Method used to close connections. This will close local connection and connection resides in messageConsumer
         */
        protected void closeConnection() {
            getUpdateListnerState(true, STATE_SHUTTING_DOWN);
            try {
                //closing the class local connection
                if (localConnection != null) {
                    try {
                        if (localConnection.isOpen()) {
                            localConnection.close();
                            log.info("RabbitMQ localConnection closed for service " + serviceName + ", Listner id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (AlreadyClosedException e) {
                        log.warn("Error while closing Connection, AlreadyClosedException, service - " + serviceName +
                                 ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing Connection, AlreadyClosedException, service - " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                        }
                    } catch (ShutdownSignalException ex) {
                        log.warn("Error while closing Connection, ShutdownSignalException, service - " + serviceName +
                                 ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing Connection, ShutdownSignalException, service - " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), ex);
                        }
                    } catch (SocketException exx) {
                        log.warn("Error while closing Connection, SocketException, service - " + serviceName +
                                 ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing Connection, SocketException, service - " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), exx);
                        }
                    } catch (IOException e) {
                        log.warn("Error while closing Connection, IOException, service - " + serviceName +
                                 ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing Connection, IOException, service - " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                        }
                    }
                }

            } finally {
                localConnection = null;
            }
            Channel channel = null;
            if (queueingConsumer != null) {
                channel = queueingConsumer.getChannel();
            }
            try {
                if (channel != null) {
                    try {
                        //closing the channel and connection in queing consumer
                        if (channel.isOpen()) {
                            channel.close();
                            log.info("RabbitMQ consumer channel closed for service " + serviceName + ", Listner id - " +
                                     Thread.currentThread().getId());
                        }
                        if (channel.getConnection() != null && channel.getConnection().isOpen()) {
                            channel.getConnection().close();
                            log.info("RabbitMQ consumer connection closed for service " + serviceName + ", Listner id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (AlreadyClosedException e) {
                        log.warn("Error while closing consumer Connection, AlreadyClosedException, service - " + serviceName +
                                 ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing consumer Connection, AlreadyClosedException, service - " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                        }
                    } catch (ShutdownSignalException ex) {
                        log.warn("Error while closing consumer Connection, ShutdownSignalException, service - " + serviceName +
                                 ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing consumer Connection, ShutdownSignalException, service - " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), ex);
                        }
                    } catch (SocketException exx) {
                        log.warn("Error while closing consumer Connection, SocketException, service - " + serviceName +
                                 ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing consumer Connection, SocketException, service - " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), exx);
                        }
                    } catch (TimeoutException e) {
                        log.warn("Error while closing consumer Connection, TimeoutException, service - " + serviceName +
                                ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing consumer Connection, TimeoutException, service - " +
                                    serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                        }
                    } catch (IOException e) {
                        log.warn("Error while closing consumer Connection, IOException, service - " + serviceName +
                                 ", Listner id - " + Thread.currentThread().getId());
                        if (log.isDebugEnabled()) {
                            log.debug("Error while closing consumer Connection, IOException, service - " +
                                      serviceName + ", Listner id - " + Thread.currentThread().getId(), e);
                        }
                    }
                }
            } finally {
                channel = null;
            }
            getUpdateListnerState(true, STATE_STOPPED);
        }

        /**
         * Helper method to get or update listner states
         *
         * @param update
         * @param state
         * @return listnerState
         */
        private int getUpdateListnerState(boolean update, int state) {
            synchronized (ServiceTaskManager.this) {
                if (update) {
                    this.listnerState = state;
                }
                return this.listnerState;
            }
        }

        /**
         * Helper method to check whether listner is active or not
         *
         * @return true if listner is active, false otherwise
         */
        private boolean isListnerActive() {
            if (this.listnerState == STATE_STARTED) {
                return true;
            }
            return false;
        }

        /**
         * Helper method which will check whether both listner and service task manager in active state. If not
         * update listner state to STATE_STOPPED as well
         * @return true if both are in STATE_STARTED, false otherwise
         */
        private boolean isServiceTaskManagerActive() {
            if (getUpdateListnerState(false, STATE_DUMMY) != STATE_STARTED ||
                serviceTaskManagerState.getUpdateState(false, STATE_DUMMY) != STATE_STARTED) {
                getUpdateListnerState(true, STATE_STOPPED);
                return false;

            }
            return true;
        }

    }
}

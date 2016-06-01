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
import java.util.concurrent.atomic.AtomicInteger;


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
    private static final int STATE_FAILURE = 5;
    private static final int STATE_FAULTY = 6;

    /**
     * Number of concurrent consumers - for PubSub, this should be 1 to prevent multiple receipt
     */
    private int concurrentConsumers = RabbitMQConstants.CONCURRENT_CONSUMER_COUNT_DEFAULT;

    /**
     * Number of invoker tasks active
     */
    private AtomicInteger activeTaskCount = new AtomicInteger(0);

    /**
     * The JMS Connection shared between multiple polling tasks - when enabled (reccomended)
     */
    private Connection sharedConnection = null;

    /**
     * JMS Resource cache level - Connection, Session, Consumer. Auto will select safe default
     */
    private volatile int cacheLevel = RabbitMQConstants.CACHE_NONE;

    private WorkerPool workerPool = null;
    private String serviceName;
    private Hashtable<String, String> rabbitMQProperties = new Hashtable<>();
    private final RabbitMQConnectionFactory rabbitMQConnectionFactory;
    private final List<MessageListenerTask> pollingTasks =
            Collections.synchronizedList(new ArrayList<MessageListenerTask>());
    private RabbitMQMessageReceiver rabbitMQMessageReceiver;
    private volatile int serviceTaskManagerState = STATE_STOPPED;

    public ServiceTaskManager(
            RabbitMQConnectionFactory rabbitMQConnectionFactory) {
        this.rabbitMQConnectionFactory = rabbitMQConnectionFactory;
    }

    /**
     * Start  the Task Manager by adding a new MessageListenerTask to the worker pool.
     */
    public synchronized void start() {
        serviceTaskManagerState = STATE_STARTING;
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
        serviceTaskManagerState = STATE_STARTED;
    }

    /**
     * Stop the consumer by changing the state
     */
    public void stop() {
        serviceTaskManagerState = STATE_SHUTTING_DOWN;

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
        serviceTaskManagerState = STATE_STOPPED;
    }

    public synchronized void pause() {
        //TODO implement me ..
    }

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

    /**
     * Helper method to close shared connection if we use connection caching
     */
    private void closeSharedConnection() {
        if (sharedConnection != null) {
            try {
                if (sharedConnection.isOpen()) {
                    sharedConnection.close();
                    log.info("RabbitMQ sharedConnection closed for service " + serviceName);
                }
            } catch (IOException e) {
                log.error("Error while closing RabbitMQ sharedConnection for service " + serviceName, e);
            } catch (AlreadyClosedException e) {
                if (serviceTaskManagerState == STATE_STARTED) {
                    log.error("Error sharedConnection already closed " + serviceName, e);
                } else {
                    log.warn("Error sharedConnection already closed " + serviceName);
                }
            } finally {
                sharedConnection = null;
            }
        }
        sharedConnection = null;
    }

    /**
     * The actual threads/tasks that perform message polling
     */
    private class MessageListenerTask implements Runnable {

        private Connection connection = null;
        private boolean autoAck = false;
        private volatile RMQChannel rmqChannel = null;
        private volatile int workerState = STATE_STOPPED;
        private volatile boolean idle = false;
        private volatile boolean connected = false;
        private String queueName, routeKey, exchangeName;
        private volatile QueueingConsumer queueingConsumer;
        private String consumerTagString;

        /**
         * As soon as we send a new polling task, add it to the STM for control later
         */
        MessageListenerTask() {
            synchronized (pollingTasks) {
                pollingTasks.add(this);
            }
        }

        public void pause() {
            //TODO implement me
        }

        public void resume() {
            //TODO implement me
        }

        /**
         * Execute the polling worker task
         * Actual reconnection is not happening in here, re-connection is handled by  the rabbitmq connection
         * itself. The only use of recovery interval in here is that to output the info message
         */
        public void run() {
            /**
             * This happens when message listner count is larger than thread pool size
             * Then when we stop the message receiver, it will stop currently running listner threads,
             * so threads which were waiting will start. This check is to stop that behavior
             */
            if (serviceTaskManagerState == STATE_SHUTTING_DOWN || serviceTaskManagerState == STATE_STOPPED) {
                return;
            }
            workerState = STATE_STARTED;
            activeTaskCount.getAndIncrement();
            try {
                initConsumer();
                while (workerState == STATE_STARTED) {
                    try {
                        startConsumer();
                    } catch (AlreadyClosedException e) {
                        if (isActive() || serviceTaskManagerState == STATE_STARTED) {
                            log.error("Error Connection already closed " + serviceName + ", Thread id - " +
                                      Thread.currentThread().getId(), e);
                            waitForConnection();
                        } else {
                            log.warn("Error Connection already closed " + serviceName + ", Thread id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (ShutdownSignalException sse) {
                        if (!sse.isInitiatedByApplication()) {
                            log.error("RabbitMQ Listener of the service " + serviceName +
                                      " was disconnected, Thread id - " + Thread.currentThread().getId(), sse);
                            waitForConnection();
                        } else {
                            workerState = STATE_FAILURE;
                        }
                    } catch (OMException e) {
                        log.error("Invalid Message Format while Consuming the message, Thread id - " +
                                  Thread.currentThread().getId(), e);
                    } catch (SocketException exx) {
                        if (!isActive() || serviceTaskManagerState != STATE_STARTED) {
                            if (log.isDebugEnabled()) {
                                log.debug("RabbitMQ listner disconnected, service - " + serviceName +
                                          ", Thread id - " + Thread.currentThread().getId(), exx);
                            }
                        } else {
                            log.error("RabbitMQ listner disconnected, service - " + serviceName +
                                      ", Thread id - " + Thread.currentThread().getId(), exx);
                        }
                    } catch (IOException e) {
                        if (!handleIOException("RabbitMQ listner was disconnected", e)) {
                            waitForConnection();
                        }
                    }
                }
            } catch (SocketException exx) {
                if (!isActive() || serviceTaskManagerState != STATE_STARTED) {
                    if (log.isDebugEnabled()) {
                        log.debug("Error initializing RabbitMQ consumer, service - " + serviceName +
                                  ", Thread id - " + Thread.currentThread().getId(), exx);
                    }
                } else {
                    log.error("Error initializing RabbitMQ consumer, service - " + serviceName +
                              ", Thread id - " + Thread.currentThread().getId(), exx);
                }
            } catch (IOException e) {
                if (!handleIOException("Error initializing consumer for service", e)) {
                    handleException("Error initializing consumer for service " + serviceName + ", Thread id - " +
                                    Thread.currentThread().getId(), e);
                }
            } finally {
                closeConnection();
                workerState = STATE_STOPPED;
                activeTaskCount.getAndDecrement();
                synchronized (pollingTasks) {
                    pollingTasks.remove(this);
                }
            }
        }

        private void waitForConnection() throws IOException {
            int retryInterval = rabbitMQConnectionFactory.getRetryInterval();
            int retryCountMax = rabbitMQConnectionFactory.getRetryCount();
            int retryCount = 0;
            while ((workerState == STATE_STARTED && serviceTaskManagerState == STATE_STARTED) && !connection.isOpen()
                   && ((retryCountMax == -1) || (retryCount < retryCountMax))) {
                retryCount++;
                log.info("Attempting to reconnect to RabbitMQ Broker for the service " + serviceName +
                         " in " + retryInterval + " ms, Thread id - " + Thread.currentThread().getId());
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e) {
                    log.error("Error while trying to reconnect to RabbitMQ Broker for the service - " + serviceName +
                              ", Thread id - " + Thread.currentThread().getId(), e);
                }
            }
            if (!isActive() || serviceTaskManagerState != STATE_STARTED) {
                if (log.isDebugEnabled()) {
                    log.debug("Service deactivated during waiting period, service - " + serviceName +
                              ", Thread id - " + Thread.currentThread().getId());
                }
                workerState = STATE_SHUTTING_DOWN;
                return;
            } else if (connection.isOpen()) {
                log.info("Successfully reconnected to RabbitMQ Broker for the service " + serviceName +
                         ", Thread id - " + Thread.currentThread().getId());
                initConsumer();
            } else {
                log.error("Could not reconnect to the RabbitMQ Broker for the service " + serviceName +
                          ". Connection is closed, Thread id - " + Thread.currentThread().getId());
                workerState = STATE_FAULTY;
            }
        }

        /**
         * Used to start message consuming messages. This method is called in startup and when
         * connection is re-connected. This method will request for the connection and create
         * channel, queues, exchanges and bind queues to exchanges before consuming messages
         *
         * @throws ShutdownSignalException
         * @throws IOException
         */
        private void startConsumer() throws ShutdownSignalException, IOException {
            Channel channel = rmqChannel.getChannel();

            //unable to connect to the queue
            if (queueingConsumer == null) {
                workerState = STATE_STOPPED;
                return;
            }

            while (isActive()) {
                try {
                    if (!channel.isOpen()) {
                        channel = queueingConsumer.getChannel();
                    }
                    channel.txSelect();
                } catch (SocketException exx) {
                    if (!isActive() || serviceTaskManagerState != STATE_STARTED) {
                        if (log.isDebugEnabled()) {
                            log.debug("Error while starting transaction, service - " + serviceName +
                                      ", Thread id - " + Thread.currentThread().getId(), exx);
                        }
                    } else {
                        log.error("Error while starting transaction, service - " + serviceName +
                                  ", Thread id - " + Thread.currentThread().getId(), exx);
                    }
                } catch (IOException e) {
                    if (handleIOException("Error while starting transaction", e)) {
                        break;
                    }
                    continue;
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
                    idle = false;
                    try {
                        successful = handleMessage(message);
                    } finally {
                        if (successful) {
                            try {
                                if (!autoAck) {
                                    channel.basicAck(message.getDeliveryTag(), false);
                                }
                                channel.txCommit();
                            } catch (SocketException exx) {
                                if (!isActive() || serviceTaskManagerState != STATE_STARTED) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Error while committing transaction, service - " + serviceName +
                                                  ", Thread id - " + Thread.currentThread().getId(), exx);
                                    }
                                } else {
                                    log.error("Error while committing transaction, service - " + serviceName +
                                              ", Thread id - " + Thread.currentThread().getId(), exx);
                                }
                            } catch (IOException e) {
                                if (handleIOException("Error while committing transaction", e)) {
                                    break;
                                }
                            }
                        } else {
                            try {
                                channel.txRollback();
                            } catch (SocketException exx) {
                                if (!isActive() || serviceTaskManagerState != STATE_STARTED) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Error while trying to roll back transaction, service - " + serviceName +
                                                  ", Thread id - " + Thread.currentThread().getId(), exx);
                                    }
                                } else {
                                    log.error("Error while trying to roll back transaction, service - " + serviceName +
                                              ", Thread id - " + Thread.currentThread().getId(), exx);
                                }
                            } catch (IOException e) {
                                if (handleIOException("Error while trying to roll back transaction", e)) {
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    idle = true;
                }
            }

        }

        /**
         * Create a queue consumer using the properties form transport listener configuration
         *
         * @throws IOException on error
         */
        private void initConsumer() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Initializing consumer for service " + serviceName);
            }

            int qosValue = -1;

            //set the qos value for the RMQ channel so it will be applied when getting a channel back
            String qos = rabbitMQProperties.get(RabbitMQConstants.CONSUMER_QOS);
            if (qos != null && !"".equals(qos)) {
                try {
                    qosValue = Integer.parseInt(qos);
                } catch (NumberFormatException e) {
                    qosValue = -1;
                    log.warn("Can't parse given RabbitMQ qos value as a integer, hence using " +
                             "channel without qos, provided qos value - " + qos);
                }
            }
            connection = getConnection();
            rmqChannel = new RMQChannel(connection, qosValue);
            queueName = rabbitMQProperties.get(RabbitMQConstants.QUEUE_NAME);
            routeKey = rabbitMQProperties.get(RabbitMQConstants.QUEUE_ROUTING_KEY);
            exchangeName = rabbitMQProperties.get(RabbitMQConstants.EXCHANGE_NAME);

            if (log.isDebugEnabled()) {
                log.debug("Starting MessageListner Thread - " + Thread.currentThread().getId() +
                          ", with channel(hashcode) - " + rmqChannel.getChannel().hashCode());
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
                RabbitMQUtils.declareQueue(rmqChannel, queueName, rabbitMQProperties);
            }

            if (exchangeAutoDeclare && !StringUtils.isEmpty(exchangeName)) {
                //declaring exchange
                RabbitMQUtils.declareExchange(rmqChannel, exchangeName, rabbitMQProperties);
                rmqChannel.getChannel().queueBind(queueName, exchangeName, routeKey);
                if (log.isDebugEnabled()) {
                    log.debug("Bind queue '" + queueName + "' to exchange '" + exchangeName + "' with route key '" + routeKey + "'");
                }
            }


            queueingConsumer = new QueueingConsumer(rmqChannel.getChannel());

            consumerTagString = rabbitMQProperties.get(RabbitMQConstants.CONSUMER_TAG);
            if (consumerTagString != null) {
                rmqChannel.getChannel().basicConsume(queueName, autoAck, consumerTagString, queueingConsumer);
                if (log.isDebugEnabled()) {
                    log.debug("Start consuming queue '" + queueName + "' with consumer tag '" + consumerTagString + "' for service " + serviceName);
                }
            } else {
                consumerTagString = rmqChannel.getChannel().basicConsume(queueName, autoAck, queueingConsumer);
                if (log.isDebugEnabled()) {
                    log.debug("Start consuming queue '" + queueName + "' with consumer tag '" + consumerTagString + "' for service " + serviceName);
                }
            }
        }

        /**
         * Returns the delivery from the consumer
         *
         * @param consumer the consumer to get the delivery
         * @return RabbitMQMessage consumed by the consumer
         * @throws InterruptedException on error
         */
        private RabbitMQMessage getConsumerDelivery(QueueingConsumer consumer)
                throws InterruptedException, ShutdownSignalException {
            RabbitMQMessage message = new RabbitMQMessage();
            QueueingConsumer.Delivery delivery = null;
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Waiting for next delivery from queue for service " + serviceName +
                              ", Thread id - " + Thread.currentThread().getId());
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
         * Invoke message receiver on received messages
         *
         * @param message the AMQP message received
         */
        private boolean handleMessage(RabbitMQMessage message) {
            boolean successful;
            successful = rabbitMQMessageReceiver.onMessage(message);
            return successful;
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

        private Connection getConnection() throws IOException {
            if (cacheLevel < RabbitMQConstants.CACHE_CONNECTION) {
                // Connection is not shared
                if (connection == null) {
                    connection = createConnection();
                    setConnected(true);
                }
            } else if (connection == null) {
                // Connection is shared, but may not have been created
                synchronized (ServiceTaskManager.this) {
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
         * Helper method to change status when stop command issued
         */
        private void changeWorkerStatus() {
            if (serviceTaskManagerState == STATE_SHUTTING_DOWN || serviceTaskManagerState == STATE_STOPPED) {
                workerState = STATE_SHUTTING_DOWN;
            }
        }

        /**
         * Helper method to close connection in RMQChannel and local
         */
        public void closeConnection() {
            workerState = STATE_SHUTTING_DOWN;
            try {
                //closing the class local connection
                if (connection != null) {
                    try {
                        if (connection.isOpen()) {
                            connection.close();
                            log.info("RabbitMQ connection closed for service " + serviceName + ", Thread id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (AlreadyClosedException e) {
                        if (isActive()) {
                            log.error("Error Connection already closed " + serviceName + ", Thread id - " +
                                      Thread.currentThread().getId(), e);
                        } else {
                            log.warn("Error Connection already closed " + serviceName + ", Thread id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (ShutdownSignalException ex) {
                        if (!ex.isInitiatedByApplication()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Error while closing RabbitMQ connection for service - " + serviceName +
                                          ", Thread id - " + Thread.currentThread().getId(), ex);
                            }
                        } else {
                            log.warn("Error while closing RabbitMQ connection for service - " + serviceName + ", Thread id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (SocketException exx) {
                        if (log.isDebugEnabled()) {
                            log.debug("Error closing RabbitMQ connection, socket already closed, service - " + serviceName +
                                      ", Thread id - " + Thread.currentThread().getId());
                        }
                    } catch (IOException e) {
                        if (handleIOException("Error while closing RabbitMQ connection for service", e)) {
                            return;
                        }
                    }
                }

            } finally {
                connection = null;
            }


            Channel channel = queueingConsumer.getChannel();
            if (channel != null) {
                try {
                    //closing the channel and connection in queing consumer
                    if (channel.isOpen()) {
                        channel.close();
                        log.info("RabbitMQ RMQ Channel closed for service " + serviceName + ", Thread id - " +
                                 Thread.currentThread().getId());
                    }
                    if (channel.getConnection() != null && channel.getConnection().isOpen()) {
                        channel.getConnection().close();
                        log.info("RabbitMQ RMQ Channel connection closed for service " + serviceName + ", Thread id - " +
                                 Thread.currentThread().getId());
                    }
                } catch (AlreadyClosedException e) {
                    if (isActive()) {
                        log.error("Error RMQ channel already closed, service - " + serviceName + ", Thread id - " +
                                  Thread.currentThread().getId(), e);
                    } else {
                        log.warn("Error RMQ channel already closed, service - " + serviceName + ", Thread id - " +
                                 Thread.currentThread().getId());
                    }
                } catch (ShutdownSignalException ex) {
                    if (!ex.isInitiatedByApplication()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Error RMQ channel already closed, service - " + serviceName +
                                      ", Thread id - " + Thread.currentThread().getId(), ex);
                        }
                    } else {
                        log.warn("Error RMQ channel already closed, service - " + serviceName + ", Thread id - " +
                                 Thread.currentThread().getId());
                    }
                } catch (SocketException exx) {
                    if (log.isDebugEnabled()) {
                        log.debug("Error closing RMQ channel, socket already closed, service - " + serviceName +
                                  ", Thread id - " + Thread.currentThread().getId());
                    }
                } catch (IOException e) {
                    if (handleIOException("Error while closing RabbitMQ RMQ channel for service", e)) {
                        return;
                    }
                } finally {
                    channel = null;
                }
            }
            channel = null;

            try {
                if (rmqChannel != null) {
                    try {
                        //closing the connection in rmqchannel
                        if (rmqChannel.isOpen()) {
                            rmqChannel.closeConnection();
                            log.info("RabbitMQ RMQ Channel connection closed for service " + serviceName + ", Thread id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (AlreadyClosedException e) {
                        if (isActive()) {
                            log.error("Error RMQ Connection already closed " + serviceName + ", Thread id - " +
                                      Thread.currentThread().getId(), e);
                        } else {
                            log.warn("Error RMQ Connection already closed " + serviceName + ", Thread id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (ShutdownSignalException ex) {
                        if (!ex.isInitiatedByApplication()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Error while closing RabbitMQ RMQ connection for service - " + serviceName +
                                          ", Thread id - " + Thread.currentThread().getId(), ex);
                            }
                        } else {
                            log.warn("Error while closing RabbitMQ RMQ connection for service - " + serviceName + ", Thread id - " +
                                     Thread.currentThread().getId());
                        }
                    } catch (SocketException exx) {
                        if (log.isDebugEnabled()) {
                            log.debug("Error closing RMQ connection, socket already closed, service - " + serviceName +
                                      ", Thread id - " + Thread.currentThread().getId());
                        }
                    } catch (IOException e) {
                        if (handleIOException("Error while closing RabbitMQ RMQ connection for service", e)) {
                            return;
                        }
                    }
                }
            } finally {
                rmqChannel = null;
            }

        }

        private Connection createConnection() throws IOException {
            Connection connection = null;
            try {
                connection = rabbitMQConnectionFactory.createConnection();
                log.info("RabbitMQ connection created for service " + serviceName);
            } catch (Exception e) {
                handleException("Error while creating RabbitMQ connection for service " + serviceName, e);
            }
            return connection;
        }

        /**
         * Helper method to handle IOExceptions, this will return true if a flow this is called should continue and
         * false otherwise
         *
         * @param message
         * @param exception
         * @return true or false
         */
        private boolean handleIOException(String message, IOException exception) {
            String fullMessage = message + ", Shutdown signal issued, service - " + serviceName + ", Thread id - " +
                                 Thread.currentThread().getId();
            if (exception.getCause() instanceof ShutdownSignalException) {
                if (((ShutdownSignalException) exception.getCause()).isInitiatedByApplication()) {
                    changeWorkerStatus();
                    log.warn(fullMessage);
                    if (log.isDebugEnabled()) {
                        log.debug(fullMessage, exception);
                    }
                    return true;
                } else if (!isActive() || serviceTaskManagerState != STATE_STARTED) {
                    changeWorkerStatus();
                    log.warn(fullMessage);
                    if (log.isDebugEnabled()) {
                        log.debug(fullMessage, exception);
                    }
                    return true;
                }
            }
            log.error(fullMessage, exception);
            return false;
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new AxisRabbitMQException(msg, e);
    }

}
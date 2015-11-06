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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;


/**
 * Each service will have one ServiceTaskManager instance that will send, manage and also destroy
 * idle tasks created for it, for message receipt. It uses the MessageListenerTask to poll for the
 * RabbitMQ AMQP Listening destination and consume messages. The consumed messages is build and sent to
 * axis2 engine for processing
 */

public class ServiceTaskManager {
    private static final Log log = LogFactory.getLog(ServiceTaskManager.class);

    private static final int STATE_STOPPED = 0;
    private static final int STATE_STARTED = 1;
    private static final int STATE_PAUSED = 2;
    private static final int STATE_SHUTTING_DOWN = 3;
    private static final int STATE_FAILURE = 4;
    private static final int STATE_FAULTY = 5;
    private volatile int activeTaskCount = 0;

    private WorkerPool workerPool = null;
    private String serviceName;
    private Hashtable<String, String> rabbitMQProperties = new Hashtable<>();
    private final RabbitMQConnectionFactory rabbitMQConnectionFactory;
    private final List<MessageListenerTask> pollingTasks =
            Collections.synchronizedList(new ArrayList<MessageListenerTask>());
    private RabbitMQMessageReceiver rabbitMQMessageReceiver;
    private int serviceTaskManagerState = STATE_STOPPED;

    public ServiceTaskManager(
            RabbitMQConnectionFactory rabbitMQConnectionFactory) {
        this.rabbitMQConnectionFactory = rabbitMQConnectionFactory;
    }

    /**
     * Start  the Task Manager by adding a new MessageListenerTask to the worker pool.
     */
    public synchronized void start() {
        workerPool.execute(new MessageListenerTask());
        serviceTaskManagerState = STATE_STARTED;
    }

    /**
     * Stop the consumer by changing the state
     */
    public synchronized void stop() {
        if (serviceTaskManagerState != STATE_FAILURE) {
            serviceTaskManagerState = STATE_SHUTTING_DOWN;
        }

        synchronized (pollingTasks) {
            for (MessageListenerTask lstTask : pollingTasks) {
                lstTask.requestShutdown();
            }
        }

        if (serviceTaskManagerState != STATE_FAILURE) {
            serviceTaskManagerState = STATE_STOPPED;
        }
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
     * The actual threads/tasks that perform message polling
     */
    private class MessageListenerTask implements Runnable {

        private Connection connection = null;
        private boolean autoAck = false;
        private RMQChannel rmqChannel = null;
        private volatile int workerState = STATE_STOPPED;
        private volatile boolean idle = false;
        private volatile boolean connected = false;
        private String queueName, routeKey, exchangeName;
        private QueueingConsumer queueingConsumer;
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
            workerState = STATE_STARTED;
            activeTaskCount++;
            try {
                initConsumer();
                while (workerState == STATE_STARTED) {
                    try {
                        startConsumer();
                    } catch (ShutdownSignalException sse) {
                        if (!sse.isInitiatedByApplication()) {
                            log.error("RabbitMQ Listener of the service " + serviceName +
                                    " was disconnected", sse);
                            waitForConnection();
                        }
                    } catch (OMException e) {
                        log.error("Invalid Message Format while Consuming the message", e);
                    } catch (IOException e) {
                        log.error("RabbitMQ Listener of the service " + serviceName +
                                " was disconnected", e);
                        waitForConnection();
                    }
                }
            } catch (IOException e) {
                handleException("Error initializing consumer for service " + serviceName, e);
            } finally {
                closeConnection();
                workerState = STATE_STOPPED;
                activeTaskCount--;
                synchronized (pollingTasks) {
                    pollingTasks.remove(this);
                }
            }
        }

        private void waitForConnection() throws IOException {
            int retryInterval = rabbitMQConnectionFactory.getRetryInterval();
            int retryCountMax = rabbitMQConnectionFactory.getRetryCount();
            int retryCount = 0;
            while ((workerState == STATE_STARTED) && !connection.isOpen()
                    && ((retryCountMax == -1) || (retryCount < retryCountMax))) {
                retryCount++;
                log.info("Attempting to reconnect to RabbitMQ Broker for the service " + serviceName +
                        " in " + retryInterval + " ms");
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e) {
                    log.error("Error while trying to reconnect to RabbitMQ Broker for the service " +
                            serviceName, e);
                }
            }
            if (connection.isOpen()) {
                log.info("Successfully reconnected to RabbitMQ Broker for the service " + serviceName);
                initConsumer();
            } else {
                log.error("Could not reconnect to the RabbitMQ Broker for the service " + serviceName +
                        ". Connection is closed.");
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
            //set the qos value for the consumer
            String qos = rabbitMQProperties.get(RabbitMQConstants.CONSUMER_QOS);
            if (qos != null && !"".equals(qos)) {
                channel.basicQos(Integer.parseInt(qos));
            }

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
                } catch (IOException e) {
                    log.error("Error while starting transaction", e);
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
                            } catch (IOException e) {
                                log.error("Error while committing transaction", e);
                            }
                        } else {
                            try {
                                channel.txRollback();
                            } catch (IOException e) {
                                log.error("Error while trying to roll back transaction", e);
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
            connection = getConnection();
            rmqChannel = new RMQChannel(connection, connection.createChannel());
            queueName = rabbitMQProperties.get(RabbitMQConstants.QUEUE_NAME);
            routeKey = rabbitMQProperties.get(RabbitMQConstants.QUEUE_ROUTING_KEY);
            exchangeName = rabbitMQProperties.get(RabbitMQConstants.EXCHANGE_NAME);

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
                    log.debug("Waiting for next delivery from queue for service " + serviceName);
                }
                delivery = consumer.nextDelivery();
            } catch (ShutdownSignalException e) {
                return null;
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
            closeConnection();
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
            if (connection == null) {
                connection = createConnection();
                setConnected(true);
            }
            return connection;
        }

        private void closeConnection() {
            if (connection != null && connection.isOpen()) {
                try {
                    connection.close();
                    log.info("RabbitMQ connection closed for service " + serviceName);
                } catch (IOException e) {
                    log.error("Error while closing RabbitMQ connection for service " + serviceName, e);
                } finally {
                    connection = null;
                }
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
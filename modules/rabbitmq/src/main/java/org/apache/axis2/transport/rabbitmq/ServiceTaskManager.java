package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.axis2.util.GracefulShutdownTimer;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An instance of this class will create when AMQP listener proxy is being deployed.
 */
public class ServiceTaskManager {

    private static final Log log = LogFactory.getLog(ServiceTaskManager.class);
    private final Map<String, String> rabbitMQProperties = new HashMap<>();
    private final Map<String, String> unmodifiableRabbitMQProperties = Collections.unmodifiableMap(rabbitMQProperties);
    private final List<ServiceTaskManager.MessageListenerTask> pollingTasks =
            Collections.synchronizedList(new ArrayList<>());
    private final RabbitMQConnectionFactory rabbitMQConnectionFactory;
    private volatile RabbitMQMessageReceiver rabbitMQMessageReceiver;
    private volatile String serviceName;
    private WorkerPool workerPool = null;
    private Connection connection;
    private final String factoryName;

    /**
     * Constructor of service task manager.
     *
     * @param rabbitMQConnectionFactory {@link RabbitMQConnectionFactory} object
     * @param factoryName            connection factory name configured in the axis2.xml
     */
    public ServiceTaskManager(RabbitMQConnectionFactory rabbitMQConnectionFactory, String factoryName) {
        this.rabbitMQConnectionFactory = rabbitMQConnectionFactory;
        this.factoryName = factoryName;
    }


    /**
     * Start  the Task Manager by adding a new MessageListenerTasks to the worker pool.
     */
    public void start() throws Exception {
        // set the concurrentConsumer value so that, serviceTask manager will start that number of message listeners
        int concurrentConsumers = NumberUtils.toInt(rabbitMQProperties.get(RabbitMQConstants.CONCURRENT_CONSUMER_COUNT),
                RabbitMQConstants.CONCURRENT_CONSUMER_COUNT_DEFAULT);
        connection = rabbitMQConnectionFactory.create(factoryName);
        ((Recoverable) this.connection).addRecoveryListener(new RabbitMQRecoveryListener());

        long tmpInboundAckMaxWaitTime;

        // Inbound ACK max wait time
        Object inboundAckMaxWaitTime = rabbitMQProperties.get(RabbitMQConstants.INBOUND_ACK_MAX_WAIT_PARAM);
        if (inboundAckMaxWaitTime != null) {
            try {
                tmpInboundAckMaxWaitTime = Long.parseLong(inboundAckMaxWaitTime.toString());
                RabbitMQAckConfig.setInboundAckMaxWaitTimeMs(tmpInboundAckMaxWaitTime);
                log.info("Using RabbitMQ Inbound Ack Max Wait Time Configured at Service: " + serviceName
                        + " value of : " + RabbitMQAckConfig.getInboundAckMaxWaitTimeMs() + " ms");
            } catch (NumberFormatException e) {
                log.warn("Invalid value for " + RabbitMQConstants.INBOUND_ACK_MAX_WAIT_PARAM
                        + " in Service: " + serviceName
                        + ". Should be a valid long value. Defaulting to "
                        + RabbitMQAckConfig.getInboundAckMaxWaitTimeMs());
            }
        }

        for (int i = 0; i < concurrentConsumers; i++) {
            workerPool.execute(new ServiceTaskManager.MessageListenerTask());
        }
    }

    /**
     * Stop the consumer.
     */
    public void stop() {
        this.stop(false);
    }

    /**
     * Stop the consumer.
     *
     * @param listenerShuttingDown true if the listener is shutting down, false if an individual service is stopping
     *                             (service undeploy)
     */
    public void stop(boolean listenerShuttingDown) {
        synchronized (pollingTasks) {
            try {
                for (MessageListenerTask listenerTask : pollingTasks) {
                    listenerTask.close(listenerShuttingDown);
                }
            } finally {
                try {
                    closeSharedConnectionGracefully();
                    log.info("RabbitMQ connection closed gracefully for service: " + serviceName);
                } catch (IOException e) {
                    log.error("Error while closing RabbitMQ connection gracefully. Forcing abort.", e);
                    closeSharedConnectionForcefully();
                } finally {
                    connection = null;
                }
            }
        }
    }

    public Map<String, String> getRabbitMQProperties() {
        return unmodifiableRabbitMQProperties;
    }

    /**
     * The actual threads/tasks that perform message polling
     */
    private class MessageListenerTask implements Runnable, Consumer {

        private Channel channel;
        private String queueName;
        private boolean autoAck;
        private long maxDeadLetteredCount;
        private long requeueDelay;

        // Throttling variables
        private boolean isThrottlingEnabled;
        private RabbitMQConstants.ThrottleMode throttleMode;
        private RabbitMQConstants.ThrottleTimeUnit throttleTimeUnit;
        private int throttleCount;
        private long consumptionStartedTime;
        private int consumedMessageCount = 0;
        private long lastMessageProcessedTime = 0;

        private String consumerTag;
        private long unDeploymentWaitTimeout;
        private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
        private final AtomicInteger inflightMessages = new AtomicInteger(0);
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
        private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

        private MessageListenerTask() throws IOException {
            this.channel = connection.createChannel();
            ((Recoverable) this.channel).addRecoveryListener(new RabbitMQRecoveryListener());
            pollingTasks.add(this);
        }

        /**
         * Register a consumer to the queue
         *
         * @throws IOException
         */
        private void initConsumer() throws IOException {

            // set the qos value
            int qos = NumberUtils.toInt(rabbitMQProperties.get(RabbitMQConstants.CONSUMER_QOS),
                    RabbitMQConstants.DEFAULT_CONSUMER_QOS);
            channel.basicQos(qos);

            // declaring queue
            queueName = rabbitMQProperties.get(RabbitMQConstants.QUEUE_NAME);
            String exchangeName = rabbitMQProperties.get(RabbitMQConstants.EXCHANGE_NAME);

            try {
                RabbitMQUtils.declareQueue(channel, queueName, rabbitMQProperties);
            } catch (IOException ex) {
                channel = RabbitMQUtils.checkAndIgnoreInEquivalentParamException(connection, ex,
                        RabbitMQConstants.QUEUE, queueName);
            }
            try {
                RabbitMQUtils.declareExchange(channel, exchangeName, rabbitMQProperties);
            } catch (IOException ex) {
                channel = RabbitMQUtils.checkAndIgnoreInEquivalentParamException(connection, ex,
                        RabbitMQConstants.EXCHANGE, exchangeName);
            }
            RabbitMQUtils.bindQueueToExchange(channel, queueName, exchangeName, rabbitMQProperties);

            // get max dead-lettered count
            maxDeadLetteredCount =
                    NumberUtils.toLong(rabbitMQProperties.get(RabbitMQConstants.MESSAGE_MAX_DEAD_LETTERED_COUNT));

            // get requeue delay
            requeueDelay =
                    NumberUtils.toLong(rabbitMQProperties.get(RabbitMQConstants.MESSAGE_REQUEUE_DELAY));

            // get consumer tag if given
            String consumerTag = rabbitMQProperties.get(RabbitMQConstants.CONSUMER_TAG);

            autoAck = BooleanUtils.toBooleanDefaultIfNull(BooleanUtils.toBooleanObject(rabbitMQProperties
                    .get(RabbitMQConstants.QUEUE_AUTO_ACK)), true);

            // Get throttle configurations if throttling is enabled
            isThrottlingEnabled = Boolean.parseBoolean(rabbitMQProperties.getOrDefault(
                    RabbitMQConstants.RABBITMQ_PROXY_THROTTLE_ENABLED, "false"));
            if (isThrottlingEnabled) {
                this.throttleMode = RabbitMQConfigUtils.getThrottleMode(rabbitMQProperties);
                this.throttleTimeUnit = RabbitMQConfigUtils.getThrottleTimeUnit(rabbitMQProperties);
                this.throttleCount = RabbitMQConfigUtils.getThrottleCount(rabbitMQProperties);
            }

            if (StringUtils.isNotEmpty(consumerTag)) {
                this.consumerTag = channel.basicConsume(queueName, autoAck, consumerTag, this);
            } else {
                this.consumerTag = channel.basicConsume(queueName, autoAck, this);
            }

            unDeploymentWaitTimeout = NumberUtils.toLong(
                    rabbitMQProperties.get(RabbitMQConstants.UNDEPLOYMENT_GRACE_TIMEOUT), 0);
        }

        /**
         * Called when the consumer is registered by a call to any of the {@link Channel#basicConsume} methods.
         *
         * @param consumerTag the consumer tag associated with the consumer
         */
        @Override
        public void handleConsumeOk(String consumerTag) {
            log.info("Start consuming queue: " + queueName + " with consumer tag: " + consumerTag +
                    " for service: " + serviceName);
        }

        /**
         * Called when the consumer is cancelled by a call to {@link Channel#basicCancel}.
         *
         * @param consumerTag the consumer tag associated with the consumer
         */
        @Override
        public void handleCancelOk(String consumerTag) {
            log.info("The consumer with consumer tag: " + consumerTag + " stops listening to new messages.");
        }

        /**
         * Called when the consumer is cancelled for reasons other than by a call to {@link Channel#basicCancel}.
         * For example, the queue has been deleted.
         * See {@link #handleCancelOk} for notification of consumer cancellation due to {@link Channel#basicCancel}.
         *
         * @param consumerTag the consumer tag associated with the consumer
         * @throws IOException
         */
        @Override
        public void handleCancel(String consumerTag) throws IOException {
            log.info("The consumer with consumer tag: " + consumerTag + " unexpectedly stops listening to new messages.");
        }

        /**
         * Called when either the channel or the underlying connection has been shut down.
         *
         * @param consumerTag the consumer tag associated with the consumer
         * @param signal      a {@link ShutdownSignalException} indicating the reason for the shut down
         */
        @Override
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException signal) {
            if (signal.isInitiatedByApplication()) {
                log.info("The connection to the messaging server was shut down. Consumer tag " + consumerTag);

            } else if (signal.getReference() instanceof Channel) {
                int channelNumber = ((Channel) signal.getReference()).getChannelNumber();
                log.info("The consumer on channel number: " + channelNumber + " with consumer tag: " + consumerTag
                        + " was shut down.");

            } else {
                log.info("The consumer with consumer tag: " + consumerTag + " was shut down.");
            }
        }

        /**
         * Called when a basic.recover-ok is received in reply to a basic.recover. All messages received before this is
         * invoked that haven't been ack'ed will be re-delivered. All messages received afterwards won't be.
         *
         * @param consumerTag the consumer tag associated with the consumer
         */
        @Override
        public void handleRecoverOk(String consumerTag) {

        }

        /**
         * Called when a basic.deliver is received for this consumer.
         *
         * @param consumerTag the consumer tag associated with the consumer
         * @param envelope    packaging data for the message
         * @param properties  content header data for the message
         * @param body        the message body (opaque, client-specific byte array)
         * @throws IOException if the consumer encounters an I/O error while processing the message
         * @see Envelope
         */
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {
            readLock.lock();
            try {
                if (isShuttingDown.get()) {
                    /*
                     * The server is shutting down. We attempt to reject the message with requeue=true
                     * so that it goes back to the queue for redelivery.
                     *
                     * However, if the channel is already closed or closing due to shutdown,
                     * basicReject may throw an exception (e.g., NullPointerException or AlreadyClosedException).
                     *
                     * This is safe to ignore because:
                     * - The message is still unacked.
                     * - RabbitMQ will automatically requeue it once the consumer connection is closed.
                     */
                    try {
                        if (!autoAck) {
                            channel.basicReject(envelope.getDeliveryTag(), true);
                            log.debug("The rejected message with message id: " + properties.getMessageId() + " and " +
                                    "delivery tag: " + envelope.getDeliveryTag() + " on the queue: " +
                                    queueName + " since the consumer is shutting down.");
                        } else {
                            log.debug("Message with message id: " + properties.getMessageId() + " and " +
                                    "delivery tag: " + envelope.getDeliveryTag() + " on the queue: " +
                                    queueName + " received during shutdown with autoAck enabled. No action taken.");
                        }
                    } catch (Exception e) {
                        log.debug("Failed to reject message during shutdown (likely due to closed channel).", e);
                    }
                    return;
                }
                inflightMessages.incrementAndGet();
            } finally {
                readLock.unlock();
            }
            AcknowledgementMode acknowledgementMode =
                    rabbitMQMessageReceiver.processThroughAxisEngine(properties, body);

            try {
                if (isThrottlingEnabled) {
                    try {
                        switch (throttleMode) {
                            case FIXED_INTERVAL: {
                                long throttleSleepDelay = getSleepDelay();
                                long currentTime = System.currentTimeMillis();

                                // Calculate remaining sleep time based on elapsed time since last message
                                long remainingSleepTime = throttleSleepDelay;
                                if (lastMessageProcessedTime > 0) {
                                    long elapsedTime = currentTime - lastMessageProcessedTime;
                                    remainingSleepTime = throttleSleepDelay - elapsedTime;
                                }
                                if (remainingSleepTime > 0) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Sleeping " + remainingSleepTime
                                                + " ms with Fixed-Interval throttling for service :" + serviceName);
                                    }
                                    Thread.sleep(remainingSleepTime);
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug("No sleep needed for Fixed-Interval throttling - " +
                                                        "sufficient time has elapsed for service :" + serviceName);
                                    }
                                }

                                // Update last message processed time
                                lastMessageProcessedTime = System.currentTimeMillis();
                            }
                            case BATCH: {
                                if (consumedMessageCount == 0) {
                                    consumptionStartedTime = System.currentTimeMillis();
                                    if (log.isDebugEnabled()) {
                                        log.debug("Batch throttling started at " + consumptionStartedTime
                                                + " for service :" + serviceName);
                                    }
                                }

                                consumedMessageCount++;
                                if (consumedMessageCount >= throttleCount) {
                                    long consumptionDuration = System.currentTimeMillis() - consumptionStartedTime;
                                    // consumed messages have exceeded the defined count
                                    long remainingDuration = getRemainingDuration(consumptionDuration);
                                    if (remainingDuration >= 0) {
                                        // if time is remaining, we need to sleep while it exceeds
                                        if (log.isDebugEnabled()) {
                                            log.debug("Sleeping " + remainingDuration
                                                    + " ms with Batch throttling for service :" + serviceName);
                                        }
                                        Thread.sleep(remainingDuration);
                                    }
                                    consumedMessageCount = 0;
                                }
                                if (log.isDebugEnabled()) {
                                    log.debug("Consumed Message Count per min:  " + consumedMessageCount);
                                }
                                break;
                            }
                            default:
                                throw new AxisRabbitMQException("Invalid Throttling mode " + throttleMode
                                        + " specified for service : " + serviceName);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Error in sleeping with " + throttleMode + " throttling", e);
                    } catch (AxisRabbitMQException e) {
                        log.error("Invalid Throttling mode " + throttleMode + " specified for service : " + serviceName,
                                e);
                    }
                }

                switch (acknowledgementMode) {
                    case REQUEUE_TRUE:
                        try {
                            Thread.sleep(requeueDelay);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        /*
                         * If the channel is already closed or closing due to shutdown,
                         * basicReject may throw an exception (e.g., NullPointerException or AlreadyClosedException).
                         *
                         * This is safe to ignore because:
                         * - The message is still unacked.
                         * - RabbitMQ will automatically requeue it once the consumer connection is closed.
                         */
                        try {
                            channel.basicReject(envelope.getDeliveryTag(), true);
                        } catch (Exception e) {
                            log.debug("Failed to reject message during shutdown (likely due to closed channel).", e);
                        }
                        break;
                    case REQUEUE_FALSE:
                        List<HashMap<String, Object>> xDeathHeader = null;
                        if (properties != null) {
                            Map<String, Object> headers = properties.getHeaders();
                            if (headers != null) {
                                xDeathHeader = (ArrayList<HashMap<String, Object>>) headers.get("x-death");
                            }
                        }
                        // check if message has been already dead-lettered
                        if (xDeathHeader != null && xDeathHeader.size() > 0 && maxDeadLetteredCount != -1) {
                            Long count = (Long) xDeathHeader.get(0).get("count");
                            if (count <= maxDeadLetteredCount) {
                                /*
                                 * If the channel is already closed or closing due to shutdown,
                                 * basicReject may throw an exception (e.g., NullPointerException or AlreadyClosedException).
                                 *
                                 * This is safe to ignore because:
                                 * - The message is still unacked.
                                 * - RabbitMQ will automatically requeue it once the consumer connection is closed.
                                 */
                                try {
                                    channel.basicReject(envelope.getDeliveryTag(), false);
                                } catch (Exception e) {
                                    log.debug("Failed to reject message during shutdown (likely due to closed channel).", e);
                                }
                                log.info("The rejected message with message id: " + properties.getMessageId() + " and " +
                                        "delivery tag: " + envelope.getDeliveryTag() + " on the queue: " +
                                        queueName + " is dead-lettered " + count + " time(s).");
                            } else {
                                // handle the message after exceeding the max dead-lettered count
                                proceedAfterMaxDeadLetteredCount(envelope, properties, body);
                            }
                        } else {
                            // the message might be dead-lettered or discard if an error occurred in the mediation flow
                            /*
                             * If the channel is already closed or closing due to shutdown,
                             * basicReject may throw an exception (e.g., NullPointerException or AlreadyClosedException).
                             *
                             * This is safe to ignore because:
                             * - The message is still unacked.
                             * - RabbitMQ will automatically requeue it once the consumer connection is closed.
                             */
                            try {
                                channel.basicReject(envelope.getDeliveryTag(), false);
                                log.info("The rejected message with message id: " + properties.getMessageId()
                                        + " and " + "delivery tag: " + envelope.getDeliveryTag() + " on the queue: "
                                        + queueName + " will discard or dead-lettered.");
                            } catch (Exception e) {
                                log.debug("Failed to reject message during shutdown (likely due to closed channel).", e);
                            }
                        }
                        break;
                    default:
                        if (!autoAck) {
                            channel.basicAck(envelope.getDeliveryTag(), false);
                        }
                        break;
                }

            } finally {
                inflightMessages.decrementAndGet();
            }
        }

        /**
         * The message will publish to the exchange with routing key or discard
         *
         * @param envelope   packaging data for the message
         * @param properties content header data for the message
         * @param body       the message body
         * @throws IOException
         */
        private void proceedAfterMaxDeadLetteredCount(Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {
            String routingKey =
                    rabbitMQProperties.get(RabbitMQConstants.MESSAGE_ERROR_QUEUE_ROUTING_KEY);
            String exchangeName =
                    rabbitMQProperties.get(RabbitMQConstants.MESSAGE_ERROR_EXCHANGE_NAME);
            if (StringUtils.isNotEmpty(routingKey) && StringUtils.isNotEmpty(exchangeName)) {
                // publish message to the given exchange with the routing key
                channel.basicPublish(exchangeName, routingKey, properties, body);
                channel.basicAck(envelope.getDeliveryTag(), false);
                log.info("The max dead lettered count exceeded. Hence message with message id: " +
                        properties.getMessageId() + " and delivery tag: " + envelope.getDeliveryTag() +
                        " publish to the exchange: " + exchangeName + " with the routing key: " + routingKey + ".");
            } else if (StringUtils.isNotEmpty(routingKey) && StringUtils.isEmpty(exchangeName)) {
                // publish message to the default exchange with the routing key
                channel.basicPublish("", routingKey, properties, body);
                channel.basicAck(envelope.getDeliveryTag(), false);
                log.info("The max dead lettered count exceeded. Hence message with message id: " +
                        properties.getMessageId() + " and delivery tag: " + envelope.getDeliveryTag() +
                        " publish to the default exchange with the routing key: " + routingKey + ".");
            } else {
                // discard the message
                channel.basicAck(envelope.getDeliveryTag(), false);
                log.info("The max dead lettered count exceeded. " +
                        "No 'rabbitmq.message.error.queue.routing.key' specified for publishing the message. " +
                        "Hence the message with message id: " + properties.getMessageId() + " and delivery tag: " +
                        envelope.getDeliveryTag() + " on the queue: " + queueName + " will discard.");
            }
        }

        /**
         * Execute by the {@link WorkerPool}
         */
        @Override
        public void run() {
            try {
                initConsumer();
            } catch (IOException e) {
                log.error("Error occurred while initializing the consumer.", e);
            }
        }

        public void close(boolean listenerShuttingDown) {
            writeLock.lock();
            try {
                if (!isShuttingDown.compareAndSet(false, true)) {
                    return; // only shutdown once
                }
            } finally {
                writeLock.unlock();
            }

            log.info("Stopping the RABBITMQ Task Manager for service: " + serviceName);

            try {
                if (channel != null && channel.isOpen() && consumerTag != null) {
                    try {
                        channel.basicCancel(consumerTag);
                        log.info("Successfully cancelled consumer: " + consumerTag);
                    } catch (IOException e) {
                        log.warn("Failed to cancel consumer cleanly, proceeding to shutdown.", e);
                    }
                }

                GracefulShutdownTimer gracefulShutdownTimer = GracefulShutdownTimer.getInstance();
                if (listenerShuttingDown) {
                    if (gracefulShutdownTimer.isStarted()) {
                        log.info("Awaiting completion of active RABBITMQ tasks for service '" + serviceName
                                + "' during graceful shutdown.");
                        waitForGracefulTaskCompletion(gracefulShutdownTimer);
                    }
                } else {
                    long waitUntil = System.currentTimeMillis() + unDeploymentWaitTimeout;
                    while (!autoAck && (inflightMessages.get() > 0 && System.currentTimeMillis() < waitUntil)) {
                        try {
                            Thread.sleep(100); // wait until all in-flight messages are done
                        } catch (InterruptedException e) {}
                    }
                }
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
                if (inflightMessages.get() > 0) {
                    log.warn("RABBITMQ Task Manager for service: " + serviceName + " stopped with "
                            + inflightMessages.get() + " active tasks remaining");
                } else {
                    log.info("Successfully stopped the RABBITMQ Task Manager for service: " + serviceName);
                }
            } catch (Exception e) {
                log.error("An error occurred while stopping the RABBITMQ Task Manager for service: "
                                + serviceName + ". Forcing abort.", e);
                if (channel != null) {
                    try {
                        channel.abort();
                    } catch (IOException ex) {
                        //ignore
                    }
                }
            } finally {
                channel = null;
            }
        }

        /**
         * Waits for the completion of all in-flight RabbitMQ tasks during a graceful shutdown.
         * The method blocks until either all in-flight messages are processed or the graceful
         * shutdown timer expires, whichever comes first. This ensures that message processing
         * is completed as much as possible before shutting down the consumer. Also, to ensure
         * the waiting loop doesn't run indefinitely due to unexpected conditions, a fallback
         * check is also introduced.
         *
         * @param gracefulShutdownTimer the {@link GracefulShutdownTimer} instance controlling the shutdown timeout
         */
        private void waitForGracefulTaskCompletion(GracefulShutdownTimer gracefulShutdownTimer) {
            long startTimeMillis = System.currentTimeMillis();
            long timeoutMillis = gracefulShutdownTimer.getShutdownTimeoutMillis();

            // If the server is shutting down, we wait until either all in-flight messages are done
            // or the graceful shutdown timer expires (whichever comes first)
            while (inflightMessages.get() > 0 && !gracefulShutdownTimer.isExpired()) {
                try {
                    Thread.sleep(100); // wait until all in-flight messages are done
                } catch (InterruptedException e) {}

                // Safety check: Ensure the loop doesn't run indefinitely due to unexpected conditions.
                // This fallback check ensures that if the timer somehow fails to expire as expected,
                // the loop can still exit gracefully once the configured timeout period has elapsed.
                if ((System.currentTimeMillis() - startTimeMillis) >= timeoutMillis) {
                    log.warn("Graceful shutdown timer elapsed. Exiting waiting loop to prevent "
                            + "indefinite blocking.");
                    break;
                }
            }
        }

        private long getSleepDelay() {
            long sleepDelay;
            switch (throttleTimeUnit) {
                case MINUTE:
                    sleepDelay = DateUtils.MILLIS_PER_MINUTE / throttleCount;
                    break;
                case HOUR:
                    sleepDelay = DateUtils.MILLIS_PER_HOUR / throttleCount;
                    break;
                case DAY:
                    sleepDelay = DateUtils.MILLIS_PER_DAY / throttleCount;
                    break;
                default:
                    log.error("Unrecognized throttle time unit, defaulting to MINUTE.");
                    sleepDelay = DateUtils.MILLIS_PER_MINUTE / throttleCount;
                    break;
            }
            return sleepDelay;
        }

        private long getRemainingDuration(long consumptionDuration) {
            long remainingDuration;

            switch (throttleTimeUnit) {
                case HOUR:
                    remainingDuration = DateUtils.MILLIS_PER_HOUR - consumptionDuration;
                    break;
                case MINUTE:
                    remainingDuration = DateUtils.MILLIS_PER_MINUTE - consumptionDuration;
                    break;
                case DAY:
                    remainingDuration = DateUtils.MILLIS_PER_DAY - consumptionDuration;
                    break;
                default:
                    log.error("Unrecognized throttle time unit, defaulting to MINUTE.");
                    remainingDuration = DateUtils.MILLIS_PER_MINUTE - consumptionDuration;
            }
            return remainingDuration;
        }
    }

    /**
     * Close the shared connection forcefully
     */
    private void closeSharedConnectionForcefully() {
        if (connection != null) {
            connection.abort();
        }
    }

    /**
     * Close the shared connection gracefully
     *
     * @throws IOException
     */
    private void closeSharedConnectionGracefully() throws IOException {
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }

    /**
     * Get the service name
     *
     * @return the service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Set the service name
     *
     * @param serviceName the name of the service
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Set the rabbitmq message receiver
     *
     * @param rabbitMQMessageReceiver a {@link RabbitMQMessageReceiver} object
     */
    public void setRabbitMQMessageReceiver(RabbitMQMessageReceiver rabbitMQMessageReceiver) {
        this.rabbitMQMessageReceiver = rabbitMQMessageReceiver;
    }

    /**
     * Add connection factory config parameters to the map
     *
     * @param rabbitMQProperties map of connection parameters
     */
    public void addRabbitMQProperties(Map<String, String> rabbitMQProperties) {
        this.rabbitMQProperties.putAll(rabbitMQProperties);
    }

    /**
     * Set the worker pool
     *
     * @param workerPool a {@link WorkerPool} object
     */
    public void setWorkerPool(WorkerPool workerPool) {
        this.workerPool = workerPool;
    }
}

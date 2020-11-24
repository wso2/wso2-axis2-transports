package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An instance of this class will create when AMQP listener proxy is being deployed.
 */
public class ServiceTaskManager {

    private static final Log log = LogFactory.getLog(ServiceTaskManager.class);
    private final Map<String, String> rabbitMQProperties = new HashMap<>();
    private final Map<String, String> unmodifiableRabbitMQProperties = Collections.unmodifiableMap(rabbitMQProperties);
    private final List<ServiceTaskManager.MessageListenerTask> pollingTasks =
            Collections.synchronizedList(new ArrayList<>());
    private final RabbitMQConnectionPool rabbitMQConnectionPool;
    private volatile RabbitMQMessageReceiver rabbitMQMessageReceiver;
    private volatile String serviceName;
    private WorkerPool workerPool = null;
    private Connection connection;
    private final String factoryName;

    /**
     * Constructor of service task manager.
     *
     * @param rabbitMQConnectionPool {@link RabbitMQConnectionPool} object
     * @param factoryName            connection factory name configured in the axis2.xml
     */
    public ServiceTaskManager(RabbitMQConnectionPool rabbitMQConnectionPool, String factoryName) {
        this.rabbitMQConnectionPool = rabbitMQConnectionPool;
        this.factoryName = factoryName;
    }


    /**
     * Start  the Task Manager by adding a new MessageListenerTasks to the worker pool.
     */
    public void start() throws Exception {
        // set the concurrentConsumer value so that, serviceTask manager will start that number of message listeners
        int concurrentConsumers = NumberUtils.toInt(rabbitMQProperties.get(RabbitMQConstants.CONCURRENT_CONSUMER_COUNT),
                RabbitMQConstants.CONCURRENT_CONSUMER_COUNT_DEFAULT);
        connection = rabbitMQConnectionPool.borrowObject(factoryName);
        for (int i = 0; i < concurrentConsumers; i++) {
            workerPool.execute(new ServiceTaskManager.MessageListenerTask());
        }
    }

    /**
     * Stop the consumer
     */
    public void stop() {
        synchronized (pollingTasks) {
            for (MessageListenerTask listenerTask : pollingTasks) {
                listenerTask.close();
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
            RabbitMQUtils.declareQueuesExchangesAndBindings(channel, queueName, exchangeName, rabbitMQProperties);

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

            if (StringUtils.isNotEmpty(consumerTag)) {
                channel.basicConsume(queueName, autoAck, consumerTag, this);
            } else {
                channel.basicConsume(queueName, autoAck, this);
            }
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
            AcknowledgementMode acknowledgementMode =
                    rabbitMQMessageReceiver.processThroughAxisEngine(properties, body);
            switch (acknowledgementMode) {
                case REQUEUE_TRUE:
                    try {
                        Thread.sleep(requeueDelay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    channel.basicReject(envelope.getDeliveryTag(), true);
                    break;
                case REQUEUE_FALSE:
                    List<HashMap<String, Object>> xDeathHeader =
                            (ArrayList<HashMap<String, Object>>) properties.getHeaders().get("x-death");
                    // check if message has been already dead-lettered
                    if (xDeathHeader != null && xDeathHeader.size() > 0 && maxDeadLetteredCount != -1) {
                        Long count = (Long) xDeathHeader.get(0).get("count");
                        if (count <= maxDeadLetteredCount) {
                            channel.basicReject(envelope.getDeliveryTag(), false);
                            log.info("The rejected message with message id: " + properties.getMessageId() + " and " +
                                    "delivery tag: " + envelope.getDeliveryTag() + " on the queue: " +
                                    queueName + " is dead-lettered " + count + " time(s).");
                        } else {
                            // handle the message after exceeding the max dead-lettered count
                            proceedAfterMaxDeadLetteredCount(envelope, properties, body);
                        }
                    } else {
                        // the message might be dead-lettered or discard if an error occurred in the mediation flow
                        channel.basicReject(envelope.getDeliveryTag(), false);
                        log.info("The rejected message with message id: " + properties.getMessageId() + " and " +
                                "delivery tag: " + envelope.getDeliveryTag() + " on the queue: " +
                                queueName + " will discard or dead-lettered.");
                    }
                    break;
                default:
                    if (!autoAck) {
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    }
                    break;
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

        /**
         * Return connection back to the pool when undeploying the listener proxy
         */
        public void close() {
            connection.abort();
            rabbitMQConnectionPool.returnObject(factoryName, connection);
            channel = null;
            connection = null;
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

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
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.ConsumerCancelledException;

import org.apache.axis2.transport.base.threads.WorkerPool;
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
	private Hashtable<String, String> rabbitMQProperties = new Hashtable<String, String>();
	private final ConnectionFactory connectionFactory;
	private final List<MessageListenerTask> pollingTasks =
			Collections.synchronizedList(new ArrayList<MessageListenerTask>());
	private RabbitMQMessageReceiver rabbitMQMessageReceiver;
	private int serviceTaskManagerState = STATE_STOPPED;

	public ServiceTaskManager(
			ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
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
		private Channel channel = null;
		private boolean autoAck = false;

		private volatile int workerState = STATE_STOPPED;
		private volatile boolean idle = false;
		private volatile boolean connected = false;

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
                while (workerState == STATE_STARTED) {
                    try {
                        startConsumer();
                    } catch (ShutdownSignalException sse) {
                        if (!sse.isInitiatedByApplication()) {
                            log.error("RabbitMQ Listener of the service " + serviceName +
                                    " was disconnected", sse);
                            waitForConnection();
                        }
                    } catch (IOException e) {
                        log.error("RabbitMQ Listener of the service " + serviceName +
                                " was disconnected", e);
                        waitForConnection();
                    }

                }
            } finally {
                closeConnection();
				workerState = STATE_STOPPED;
				activeTaskCount--;
				synchronized (pollingTasks) {
					pollingTasks.remove(this);
				}
			}
		}

        private void waitForConnection() {
            int retryInterval = connectionFactory.getRetryInterval();
            int retryCountMax = connectionFactory.getRetryCount();
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
		 * @throws ShutdownSignalException
		 * @throws IOException
		 */
		private void startConsumer() throws ShutdownSignalException, IOException {
			connection = getConnection();
			if (channel == null || !channel.isOpen()) {
				channel = connection.createChannel();
                log.debug("Channel is not open. Creating a new channel for service " + serviceName);
			}
			//set the qos value for the consumer
			String qos = rabbitMQProperties.get(RabbitMQConstants.CONSUMER_QOS);
			if (qos != null && !"".equals(qos)) {
				channel.basicQos(Integer.parseInt(qos));
			}
			QueueingConsumer queueingConsumer = createQueueConsumer(channel);

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
		 * @return the queue consumer
		 * @throws IOException on error
		 */
		private QueueingConsumer createQueueConsumer(Channel channel) throws IOException {
			QueueingConsumer consumer = null;
			String queueName = rabbitMQProperties.get(RabbitMQConstants.QUEUE_NAME);
			String routeKey = rabbitMQProperties.get(RabbitMQConstants.QUEUE_ROUTING_KEY);
			String exchangeName = rabbitMQProperties.get(RabbitMQConstants.EXCHANGE_NAME);
			String autoAckStringValue = rabbitMQProperties.get(RabbitMQConstants.QUEUE_AUTO_ACK);
			if (autoAckStringValue != null) {
				autoAck = Boolean.parseBoolean(autoAckStringValue);
			}
			//If no queue name is specified then service name will be used as queue name
			if (queueName == null || queueName.equals("")) {
                queueName = serviceName;
                log.info("No queue name is specified for " + serviceName + ". " +
                        "Service name will be used as queue name");
			}

			if (routeKey == null) {
				log.info(
						"[ rabbitmq.queue.routing.key ] property not found. Using queue name as the " +
						"routing key.");
				routeKey = queueName;
			}

			Boolean queueAvailable = false;
			try {
				//check availability of the named queue
				//if an error is encountered, including if the queue does not exist and if the
				// queue is exclusively owned by another connection
				channel.queueDeclarePassive(queueName);
				queueAvailable = true;
			} catch (IOException e) {
				if (log.isDebugEnabled()) {
					log.debug("Queue :" + queueName + " not found or already declared exclusive. Trying to declare the queue.");
				}
			}
			//Setting queue properties
			String queueDurable = rabbitMQProperties.get(RabbitMQConstants.QUEUE_DURABLE);
			String queueExclusive = rabbitMQProperties.get(RabbitMQConstants.QUEUE_EXCLUSIVE);
			String queueAutoDelete = rabbitMQProperties.get(RabbitMQConstants.QUEUE_AUTO_DELETE);

			boolean bool_queueDurable = true;
			boolean bool_queueExclusive = false;
			boolean bool_queueAutoDelete = false;

			if (queueDurable != null && !queueDurable.equals(""))
				bool_queueDurable = Boolean.parseBoolean(queueDurable);
			if (queueExclusive != null && !queueExclusive.equals(""))
				bool_queueExclusive = Boolean.parseBoolean(queueExclusive);
			if (queueAutoDelete != null && !queueAutoDelete.equals(""))
				bool_queueAutoDelete = Boolean.parseBoolean(queueAutoDelete);

			if (!queueAvailable) {
				if (!channel.isOpen()) {
					channel = connection.createChannel();
                    log.debug("Channel is not open. Creating a new channel for service " + serviceName);
				}
				try {
					channel.queueDeclare(queueName, bool_queueDurable, bool_queueExclusive,
                            bool_queueAutoDelete, null);
					log.info("Declaring a queue with [ Name:" + queueName + " Durable:" +
                            bool_queueDurable + " Exclusive:" + bool_queueExclusive + " AutoDelete:" +
                            bool_queueAutoDelete + " ]");
				} catch (IOException e) {
					log.error("Can not consume from queue : " + queueName + ". Already declared as exclusive. Stopping consumer.");
					return null;
				}
			}

			if (exchangeName != null && !exchangeName.equals("")) {
				Boolean exchangeAvailable = false;
				try {
					//check availability of the named exchange
					//Throws:java.io.IOException - the server will raise a 404 channel exception
					// if the named exchange does not exists.
					channel.exchangeDeclarePassive(exchangeName);
					exchangeAvailable = true;
				} catch (IOException e) {
					log.info("Exchange :" + exchangeName + " not found.Declaring exchange.");
				}

				String exchangeType = rabbitMQProperties.get(RabbitMQConstants.EXCHANGE_TYPE);

				if (!channel.isOpen()) {
					channel = connection.createChannel();
                    log.debug("Channel is not open. Creating a new channel for service " + serviceName);
                }

				if (!exchangeAvailable) {
					try {
						if (exchangeType != null) {
                            String durable = rabbitMQProperties
									.get(RabbitMQConstants.EXCHANGE_DURABLE);
                            String autoDel = rabbitMQProperties
									.get(RabbitMQConstants.EXCHANGE_AUTODELETE);
							boolean isAutoDel = false;
							if (autoDel != null) {
								isAutoDel = Boolean.parseBoolean(autoDel);
							}
							if (durable != null) {
								channel.exchangeDeclare(exchangeName, exchangeType,
								                        Boolean.parseBoolean(durable), isAutoDel,
								                        false, null);
                            } else {
                                channel.exchangeDeclare(exchangeName, exchangeType, true, isAutoDel,
								                        false, null);
                            }
                        } else {
                            channel.exchangeDeclare(exchangeName, "direct", true);
						}
					} catch (IOException e) {
						handleException(
								"Error occurred while declaring the exchange: " + exchangeName, e);

					}

				}
				if (!channel.isOpen()) {
					channel = connection.createChannel();
                    log.debug("Channel is not open. Creating a new channel for service " + serviceName);
                }
                channel.queueBind(queueName, exchangeName, routeKey);
                log.debug("Bind queue '" + queueName + "' to exchange '" + exchangeName + "' with route key '" + routeKey + "'");
			}
			if (!channel.isOpen()) {
				channel = connection.createChannel();
                log.debug("Channel is not open. Creating a new channel for service " + serviceName);
            }
			consumer = new QueueingConsumer(channel);

			String consumerTagString = rabbitMQProperties.get(RabbitMQConstants.CONSUMER_TAG);
			if (consumerTagString != null) {
                channel.basicConsume(queueName, autoAck, consumerTagString, consumer);
                log.debug("Start consuming queue '" + queueName + "' with consumerTag '" + consumerTagString + "'");
            } else {
                channel.basicConsume(queueName, autoAck, consumer);
                log.debug("Start consuming queue '" + queueName + "'");
			}
			return consumer;
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
                log.debug("Waiting for next delivery from queue for service " + serviceName);
				delivery = consumer.nextDelivery();
			} catch (ShutdownSignalException sse) {
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

                // Set content.type
                // If content.type is set as a service parameter, precedence is given to that.
                String contentType = rabbitMQProperties.get(RabbitMQConstants.CONTENT_TYPE);
                if (contentType == null) {
                    contentType = properties.getContentType();
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
                log.debug("Queue delivery item is null for service " + serviceName);
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

		private Connection getConnection() throws IOException{
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

		private Connection createConnection() throws IOException{
			Connection connection = null;
			try {
				connection = connectionFactory.createConnection();
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
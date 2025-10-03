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


import org.apache.axis2.AxisFault;
import org.apache.axis2.transport.base.AbstractTransportListenerEx;
import org.apache.axis2.transport.base.BaseConstants;
import org.wso2.securevault.SecretResolver;

/**
 * The RabbitMQ AMQP Transport listener implementation. Creates {@link ServiceTaskManager} instances
 * for each service requesting exposure over AMQP, and stops these if they are undeployed / stopped.
 * A service indicates a AMQP Connection factory definition by name, which would be defined in the
 * RabbitMQListener on the axis2.xml, and this provides a way to reuse common configuration between
 * services, as well as to optimize resources utilized
 */
public class RabbitMQListener extends AbstractTransportListenerEx<RabbitMQEndpoint> {

    /**
     * The ConnectionFactoryManager which centralizes the management of defined factories
     */
    private RabbitMQConnectionFactory rabbitMQConnectionFactory;
    private RabbitMQConnectionPool rabbitMQConnectionPool;

    /**
     * Initialize the rabbitmq listener reading the transport configurations
     *
     * @throws AxisFault
     */
    @Override
    protected void doInit() throws AxisFault {
        try {
            SecretResolver secretResolver = getConfigurationContext().getAxisConfiguration().getSecretResolver();
            rabbitMQConnectionFactory = new RabbitMQConnectionFactory();
            int poolSize = RabbitMQUtils.resolveTransportDescription(getTransportInDescription(), secretResolver,
                    rabbitMQConnectionFactory);
            rabbitMQConnectionPool = new RabbitMQConnectionPool(rabbitMQConnectionFactory, poolSize);
            log.info("RabbitMQ AMQP Transport Receiver initialized...");
        } catch (AxisRabbitMQException e) {
            throw new AxisFault("Error occurred while initializing the RabbitMQ AMQP Transport Receiver. ", e);
        }
    }

    /**
     * Create rrabbitmq endpoint
     *
     * @return a {@link RabbitMQEndpoint} object
     */
    @Override
    protected RabbitMQEndpoint createEndpoint() {
        return new RabbitMQEndpoint(this, workerPool);
    }


    /**
     * Listen for AMQP messages on behalf of the given service
     *
     * @param endpoint the Axis service for which to listen for messages
     */
    @Override
    protected void startEndpoint(RabbitMQEndpoint endpoint) throws AxisFault {
        ServiceTaskManager stm = endpoint.getServiceTaskManager();
        try {
            stm.start();
        } catch (Exception e) {
            throw new AxisFault("Error occurred while starting the endpoint " + stm.getServiceName(), e);
        }
    }

    /**
     * Stops listening for messages for the service thats undeployed or stopped
     *
     * @param endpoint the service that was undeployed or stopped
     */
    @Override
    protected void stopEndpoint(RabbitMQEndpoint endpoint) {
        ServiceTaskManager stm = endpoint.getServiceTaskManager();
        if (log.isDebugEnabled()) {
            log.debug("Stopping receiver for for service : " + stm.getServiceName());
        }
        boolean listenerShuttingDown = (state == BaseConstants.STOPPED);
        stm.stop(listenerShuttingDown);
        log.info("Stopped listening for AMQP messages to service : " + endpoint.getServiceName());
    }

    /**
     * Get rabbitmq connection pool
     *
     * @return a {@link RabbitMQConnectionPool} object
     */
    public RabbitMQConnectionPool getRabbitMQConnectionPool() {
        return this.rabbitMQConnectionPool;
    }

    /**
     * Get rabbitmq connection factory
     *
     * @return a {@link RabbitMQConnectionFactory} object
     */
    public RabbitMQConnectionFactory getRabbitMQConnectionFactory() {
        return this.rabbitMQConnectionFactory;
    }

    @Override
    public void pause() throws AxisFault {

    }

    @Override
    public void resume() throws AxisFault {

    }
}

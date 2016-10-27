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

import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQConstants;
import org.apache.commons.lang.StringUtils;
import org.wso2.securevault.SecretResolver;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Class managing a set of {@link RabbitMQConnectionFactory} objects.
 */
public class RabbitMQConnectionFactoryManager {
    private final Map<String, RabbitMQConnectionFactory> connectionFactories =
            new HashMap<String, RabbitMQConnectionFactory>();

    /**
     * Construct a Connection factory manager for the RabbitMQ transport sender or receiver
     *
     * @param description
     */
    public RabbitMQConnectionFactoryManager(ParameterInclude description, SecretResolver secretResolver) {
        loadConnectionFactoryDefinitions(description, secretResolver);
    }

    /**
     * Get the connection factory that matches the given name, i.e. referring to
     * the same underlying connection factory. Used by the RabbitMQSender to determine if already
     * available resources should be used for outgoing messages. If no factory instance is
     * found then a new one will be created and added to the connection factory map
     *
     * @param props a Map of connection factory properties and name
     * @return the connection factory or null if no connection factory compatible
     * with the given properties exists
     */
    public RabbitMQConnectionFactory getConnectionFactory(Hashtable<String, String> props) {

        String connectionFactoryName = props.get(RabbitMQConstants.RABBITMQ_CON_FAC);

        if (StringUtils.isEmpty(connectionFactoryName)) {
            //add all properties to connection factory name in order to have a unique name
            connectionFactoryName =
                    props.get(RabbitMQConstants.SERVER_HOST_NAME) + "_" +
                            props.get(RabbitMQConstants.SERVER_PORT) + "_" +
                            props.get(RabbitMQConstants.SERVER_USER_NAME) + "_" +
                            props.get(RabbitMQConstants.SERVER_PASSWORD) + "_" +
                            props.get(RabbitMQConstants.SSL_ENABLED) + "_" +
                            props.get(RabbitMQConstants.SSL_VERSION) + "_" +
                            props.get(RabbitMQConstants.SERVER_VIRTUAL_HOST) + "_" +
                            props.get(RabbitMQConstants.SERVER_RETRY_INTERVAL) + "_" +
                            props.get(RabbitMQConstants.RETRY_INTERVAL) + "_" +
                            props.get(RabbitMQConstants.RETRY_COUNT) + "_" +
                            props.get(RabbitMQConstants.HEARTBEAT) + "_" +
                            props.get(RabbitMQConstants.CONNECTION_TIMEOUT) + "_" +
                            props.get(RabbitMQConstants.CONNECTION_POOL_SIZE);
        }

        //create/get connection factory
        RabbitMQConnectionFactory rabbitMQConnectionFactory = connectionFactories.get(connectionFactoryName);

        if (rabbitMQConnectionFactory == null) {
            synchronized (connectionFactories) {
                // ensure that connection factories are created only once in a concurrent environment
                rabbitMQConnectionFactory = connectionFactories.get(connectionFactoryName);
                if (rabbitMQConnectionFactory == null) {
                    rabbitMQConnectionFactory = new RabbitMQConnectionFactory(connectionFactoryName, props);
                    connectionFactories.put(rabbitMQConnectionFactory.getName(), rabbitMQConnectionFactory);
                }
            }
        }

        //initialize connection pools
        synchronized (rabbitMQConnectionFactory) {
            rabbitMQConnectionFactory.initializeConnectionPool((props.get(RabbitMQConstants.REPLY_TO_NAME) != null));
        }

        return rabbitMQConnectionFactory;
    }


    /**
     * Get the AMQP connection factory with the given name.
     *
     * @param connectionFactoryName the name of the AMQP connection factory
     * @return the AMQP connection factory or null if no connection factory with
     * the given name exists
     */
    public RabbitMQConnectionFactory getConnectionFactory(String connectionFactoryName) {
        return connectionFactories.get(connectionFactoryName);
    }

    /**
     * Create ConnectionFactory instances for the definitions in the transport configuration,
     * and add these into our collection of connectionFactories map keyed by name
     *
     * @param trpDesc the transport description for RabbitMQ AMQP
     */
    private void loadConnectionFactoryDefinitions(ParameterInclude trpDesc, SecretResolver secretResolver) {
        for (Parameter parameter : trpDesc.getParameters()) {
            RabbitMQConnectionFactory amqpConFactory = new RabbitMQConnectionFactory(parameter, secretResolver);
            connectionFactories.put(amqpConFactory.getName(), amqpConFactory);
        }
    }

    /**
     * Stop all connection factories.
     */
    public void stop() {
        for (RabbitMQConnectionFactory conFac : connectionFactories.values()) {
            conFac.stop();
        }
    }
}

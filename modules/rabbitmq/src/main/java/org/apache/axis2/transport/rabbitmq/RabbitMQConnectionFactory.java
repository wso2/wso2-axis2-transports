/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * The rabbitmq connection factory implementation for rabbitmq connection pool
 */
public class RabbitMQConnectionFactory extends BaseKeyedPooledObjectFactory<String, Connection> {

    private static final Log log = LogFactory.getLog(RabbitMQConnectionFactory.class);
    private Map<String, Map<String, String>> connectionFactoryConfigurations = new HashMap<>();
    private int retryInterval;
    private int retryCount;
    private Address[] addresses;

    /**
     * Create an instance that can be served by the pool
     *
     * @param factoryName the factory name used when constructing the connection
     * @return a connection that can be served by the pool
     * @throws Exception
     */
    @Override
    public Connection create(String factoryName) throws Exception {
        Map<String, String> parameters = connectionFactoryConfigurations.get(factoryName);
        if (null == parameters) {
            throw new AxisRabbitMQException("Configuration parameters not found");
        }
        ConnectionFactory connectionFactory = loadConnectionFactory(parameters);
        Connection connection = null;
        try {
            connection = RabbitMQUtils.createConnection(connectionFactory, addresses);
            log.info("[" + factoryName + "] Successfully connected to RabbitMQ Broker");
        } catch (IOException e) {
            log.error("[" + factoryName + "] Error creating connection to RabbitMQ Broker. " +
                    "Reattempting to connect.", e);
            connection = retry(factoryName, connectionFactory, connection);
            if (connection == null) {
                throw new AxisRabbitMQException("[" + factoryName + "] Could not connect to RabbitMQ Broker. " +
                        "Error while creating connection", e);
            }
        }
        return connection;
    }

    /**
     * Retry when could not connect to the broker
     *
     * @param factoryName       the factory name used when constructing the connection
     * @param connectionFactory a {@link ConnectionFactory} object
     * @param connection        the failure connection {@link Connection} object
     * @return the {@link Connection} object after retry completion
     */
    private Connection retry(String factoryName, ConnectionFactory connectionFactory, Connection connection) {
        int retryC = 0;
        while ((connection == null) && ((retryCount == -1) || (retryC < retryCount))) {
            retryC++;
            log.info("[" + factoryName + "] Attempting to create connection to RabbitMQ Broker" +
                    " in " + retryInterval + " ms");
            try {
                Thread.sleep(retryInterval);
                connection = RabbitMQUtils.createConnection(connectionFactory, addresses);
                log.info("[" + factoryName + "] Successfully connected to RabbitMQ Broker");
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            } catch (IOException e1) {
                log.error("[" + factoryName + "] Error while trying to reconnect to RabbitMQ Broker", e1);
            }
        }
        return connection;
    }

    /**
     * Wrap the provided connection with an implementation of PooledObject
     *
     * @param connection the connection to wrap
     * @return The provided connection, wrapped by a PooledObject
     */
    @Override
    public PooledObject<Connection> wrap(Connection connection) {
        return new DefaultPooledObject<>(connection);
    }

    /**
     * Destroy a connection no longer needed by the pool
     *
     * @param factoryName  the key used when selecting the connection
     * @param pooledObject a PooledObject wrapping the connection to be destroyed
     * @throws Exception
     */
    @Override
    public void destroyObject(String factoryName, PooledObject<Connection> pooledObject) throws Exception {
        pooledObject.getObject().abort();
    }

    /**
     * Ensures that the connection is safe to be returned by the pool
     *
     * @param factoryName  the key used when selecting the connection
     * @param pooledObject a PooledObject wrapping the connection to be validated
     * @return true if the connection is opened
     */
    @Override
    public boolean validateObject(String factoryName, PooledObject<Connection> pooledObject) {
        return pooledObject.getObject().isOpen();
    }

    /**
     * Get the connection factory configuration map
     *
     * @return map of connection factory configurations
     */
    public Map<String, Map<String, String>> getConnectionFactoryConfigurations() {
        return connectionFactoryConfigurations;
    }

    /**
     * Get configuration parameters by the factory name
     *
     * @param factoryName factory name to get the parameters
     * @return map of parameters of the connection factory
     */
    public Map<String, String> getConnectionFactoryConfiguration(String factoryName) {
        return connectionFactoryConfigurations.get(factoryName);
    }

    /**
     * Remove configuration parameters by the factory name
     *
     * @param factoryName factory name to get the parameters
     * @return map of parameters of the connection factory
     */
    public Map<String, String> removeConnectionFactoryConfiguration(String factoryName) {
        return connectionFactoryConfigurations.remove(factoryName);
    }

    /**
     * Add connection factory parameters by the connection factory name
     *
     * @param name       connection factory name
     * @param parameters connection parameters
     */
    public void addConnectionFactoryConfiguration(String name, Map<String, String> parameters) {
        this.connectionFactoryConfigurations.put(name, parameters);
    }

    /**
     * Initiate rabbitmq connection factory from the connection parameters
     *
     * @param parameters connection parameters
     * @return a {@link ConnectionFactory} object
     */
    private ConnectionFactory loadConnectionFactory(Map<String, String> parameters) throws AxisRabbitMQException {
        String hostnames = StringUtils.defaultIfEmpty(
                parameters.get(RabbitMQConstants.SERVER_HOST_NAME), ConnectionFactory.DEFAULT_HOST);
        String ports = StringUtils.defaultIfEmpty(
                parameters.get(RabbitMQConstants.SERVER_PORT), String.valueOf(ConnectionFactory.DEFAULT_AMQP_PORT));
        String username = StringUtils.defaultIfEmpty(
                parameters.get(RabbitMQConstants.SERVER_USER_NAME), ConnectionFactory.DEFAULT_USER);
        String password = StringUtils.defaultIfEmpty(
                parameters.get(RabbitMQConstants.SERVER_PASSWORD), ConnectionFactory.DEFAULT_PASS);
        String virtualHost = StringUtils.defaultIfEmpty(
                parameters.get(RabbitMQConstants.SERVER_VIRTUAL_HOST), ConnectionFactory.DEFAULT_VHOST);
        int heartbeat = NumberUtils.toInt(
                parameters.get(RabbitMQConstants.HEARTBEAT), ConnectionFactory.DEFAULT_HEARTBEAT);
        int connectionTimeout = NumberUtils.toInt(
                parameters.get(RabbitMQConstants.CONNECTION_TIMEOUT), ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT);
        int handshakeTimeout = NumberUtils.toInt(
                parameters.get(RabbitMQConstants.HANDSHAKE_TIMEOUT), ConnectionFactory.DEFAULT_HANDSHAKE_TIMEOUT);
        long networkRecoveryInterval = NumberUtils.toLong(
                parameters.get(RabbitMQConstants.NETWORK_RECOVERY_INTERVAL), ConnectionFactory.DEFAULT_NETWORK_RECOVERY_INTERVAL);
        this.retryInterval = NumberUtils.toInt(
                parameters.get(RabbitMQConstants.RETRY_INTERVAL), RabbitMQConstants.DEFAULT_RETRY_INTERVAL);
        this.retryCount = NumberUtils.toInt(
                parameters.get(RabbitMQConstants.RETRY_COUNT), RabbitMQConstants.DEFAULT_RETRY_COUNT);
        boolean sslEnabled = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBoolean(parameters.get(RabbitMQConstants.SSL_ENABLED)), false);
        boolean externalAuthEnabled = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBoolean(parameters.get(RabbitMQConstants.EXTERNAL_AUTH_MECHANISM)), false);
        String maxInboundMessageSize = parameters.get(RabbitMQConstants.MAX_INBOUND_MESSAGE_BODY_SIZE);

        String[] hostnameArray = hostnames.split(",");
        String[] portArray = ports.split(",");
        if (hostnameArray.length == portArray.length) {
            addresses = new Address[hostnameArray.length];
            for (int i = 0; i < hostnameArray.length; i++) {
                try {
                    addresses[i] = new Address(hostnameArray[i].trim(), Integer.parseInt(portArray[i].trim()));
                } catch (NumberFormatException e) {
                    throw new AxisRabbitMQException("Number format error in port number", e);
                }
            }
        } else {
            throw new AxisRabbitMQException("The number of hostnames must be equal to the number of ports");
        }

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost(virtualHost);
        connectionFactory.setRequestedHeartbeat(heartbeat);
        connectionFactory.setConnectionTimeout(connectionTimeout);
        connectionFactory.setHandshakeTimeout(handshakeTimeout);
        connectionFactory.setNetworkRecoveryInterval(networkRecoveryInterval);
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setTopologyRecoveryEnabled(true);
        if (StringUtils.isNotEmpty(maxInboundMessageSize)) {
            int maxInboundMessageSizeInt = NumberUtils.toInt(maxInboundMessageSize);
            connectionFactory.setMaxInboundMessageBodySize(maxInboundMessageSizeInt);
        }
        if (externalAuthEnabled) {
            connectionFactory.setSaslConfig(DefaultSaslConfig.EXTERNAL);
        }
        setSSL(parameters, sslEnabled, connectionFactory);
        return connectionFactory;
    }

    /**
     * Set secure socket layer configuration if enabled
     *
     * @param parameters        connection parameters
     * @param sslEnabled        ssl enabled
     * @param connectionFactory a {@link ConnectionFactory} object to set SSL options
     */
    private void setSSL(Map<String, String> parameters, boolean sslEnabled, ConnectionFactory connectionFactory) {
        try {
            if (sslEnabled) {
                String keyStoreLocation = parameters.get(RabbitMQConstants.SSL_KEYSTORE_LOCATION);
                String keyStoreType = parameters.get(RabbitMQConstants.SSL_KEYSTORE_TYPE);
                String keyStorePassword = parameters.get(RabbitMQConstants.SSL_KEYSTORE_PASSWORD);
                String trustStoreLocation = parameters.get(RabbitMQConstants.SSL_TRUSTSTORE_LOCATION);
                String trustStoreType = parameters.get(RabbitMQConstants.SSL_TRUSTSTORE_TYPE);
                String trustStorePassword = parameters.get(RabbitMQConstants.SSL_TRUSTSTORE_PASSWORD);
                String sslVersion = parameters.get(RabbitMQConstants.SSL_VERSION);

                if (StringUtils.isEmpty(keyStoreLocation) || StringUtils.isEmpty(keyStoreType) ||
                        StringUtils.isEmpty(keyStorePassword) || StringUtils.isEmpty(trustStoreLocation) ||
                        StringUtils.isEmpty(trustStoreType) || StringUtils.isEmpty(trustStorePassword)) {
                    log.info("Trustore and keystore information is not provided");
                    if (StringUtils.isNotEmpty(sslVersion)) {
                        connectionFactory.useSslProtocol(sslVersion);
                    } else {
                        log.info("Proceeding with default SSL configuration");
                        connectionFactory.useSslProtocol();
                    }
                } else {
                    char[] keyPassphrase = keyStorePassword.toCharArray();
                    KeyStore ks = KeyStore.getInstance(keyStoreType);
                    ks.load(new FileInputStream(keyStoreLocation), keyPassphrase);

                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(ks, keyPassphrase);

                    char[] trustPassphrase = trustStorePassword.toCharArray();
                    KeyStore tks = KeyStore.getInstance(trustStoreType);
                    tks.load(new FileInputStream(trustStoreLocation), trustPassphrase);

                    TrustManagerFactory tmf = TrustManagerFactory
                            .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    tmf.init(tks);

                    SSLContext c = SSLContext.getInstance(sslVersion);
                    c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

                    connectionFactory.useSslProtocol(c);
                }
            }
        } catch (Exception e) {
            log.warn("Format error in SSL enabled value. Proceeding without enabling SSL", e);
        }
    }
}

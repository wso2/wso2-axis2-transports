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

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.axis2.transport.rabbitmq.rpc.DualChannel;
import org.apache.axis2.transport.rabbitmq.rpc.DualChannelPool;
import org.apache.axis2.transport.rabbitmq.utils.AxisRabbitMQException;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQConstants;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Encapsulate a RabbitMQ AMQP Connection factory definition within an Axis2.xml
 * <p/>
 * Connection Factory definitions, allows service level parameters to be defined,
 * and re-used by each service that binds to it
 */
public class RabbitMQConnectionFactory {

    private static final Log log = LogFactory.getLog(RabbitMQConnectionFactory.class);

    private ConnectionFactory connectionFactory = null;
    private DualChannelPool dualChannelPool = null; //channel pool for request-response
    private RMQChannelPool rmqChannelPool = null; //channel pool for out only publish
    private String name;
    private Hashtable<String, String> parameters = new Hashtable<String, String>();
    private ExecutorService es = Executors.newFixedThreadPool(RabbitMQConstants.DEFAULT_THREAD_COUNT);
    private int retryInterval = RabbitMQConstants.DEFAULT_RETRY_INTERVAL;
    private int retryCount = RabbitMQConstants.DEFAULT_RETRY_COUNT;
    private int connectionPoolSize = RabbitMQConstants.DEFAULT_CONNECTION_POOL_SIZE;


    /**
     * Digest a AMQP CF definition from an axis2.xml 'Parameter' and construct
     *
     * @param parameter the axis2.xml 'Parameter' that defined the AMQP CF
     */
    public RabbitMQConnectionFactory(Parameter parameter) {
        this.name = parameter.getName();
        ParameterIncludeImpl pi = new ParameterIncludeImpl();

        try {
            pi.deserializeParameters((OMElement) parameter.getValue());
        } catch (AxisFault axisFault) {
            handleException("Error reading parameters for RabbitMQ connection factory" + name, axisFault);
        }

        for (Object o : pi.getParameters()) {
            Parameter p = (Parameter) o;
            parameters.put(p.getName(), (String) p.getValue());
        }
        initConnectionFactory();
        log.info("RabbitMQ ConnectionFactory : " + name + " initialized");
    }

    /**
     * Create a connection factory based on given parameters
     *
     * @param name       Name of the connection factory
     * @param parameters parameters containing the required to create the connection factory
     */
    public RabbitMQConnectionFactory(String name, Hashtable<String, String> parameters) {
        this.name = name;
        this.parameters = parameters;
        initConnectionFactory();
        if (log.isDebugEnabled()) {
            log.debug("RabbitMQ ConnectionFactory : " + name + " initialized");
        }
    }

    /**
     * Create a rabbit mq connection
     *
     * @return a connection to the server
     */
    public Connection createConnection() throws IOException {
        Connection connection = null;
        try {
            connection = RabbitMQUtils.createConnection(connectionFactory);
            log.info("[" + name + "] Successfully connected to RabbitMQ Broker");
        } catch (IOException e) {
            log.error("[" + name + "] Error creating connection to RabbitMQ Broker. Reattempting to connect.", e);
            int retryC = 0;
            while ((connection == null) && ((retryCount == -1) || (retryC < retryCount))) {
                retryC++;
                log.info("[" + name + "] Attempting to create connection to RabbitMQ Broker" +
                        " in " + retryInterval + " ms");
                try {
                    Thread.sleep(retryInterval);
                    connection = RabbitMQUtils.createConnection(connectionFactory);
                    log.info("[" + name + "] Successfully connected to RabbitMQ Broker");
                } catch (InterruptedException e1) {
                    log.error("[" + name + "] Error while trying to reconnect to RabbitMQ Broker", e1);
                } catch (IOException e2) {
                    log.error("[" + name + "] Error while trying to reconnect to RabbitMQ Broker", e2);
                }
            }
            if (connection == null) {
                handleException("[" + name + "] Could not connect to RabbitMQ Broker. Error while creating connection", e);
            }
        }
        return connection;
    }

    /**
     * get connection factory name
     *
     * @return connection factory name
     */
    public String getName() {
        return name;
    }

    /**
     * Set connection factory name
     *
     * @param name name to set for the connection Factory
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Catch an exception an throw a AxisRabbitMQException with message
     *
     * @param msg message to set for the exception
     * @param e   throwable to set
     */
    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new AxisRabbitMQException(msg, e);
    }

    /**
     * Catch an exception an throw a AxisRabbitMQException with message
     *
     * @param msg message to set for the exception
     */
    private void handleException(String msg) {
        log.error(msg);
        throw new AxisRabbitMQException(msg);
    }

    /**
     * Get all rabbit mq parameters
     *
     * @return a map of parameters
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Initialize connection factory
     */
    private void initConnectionFactory() {
        connectionFactory = new ConnectionFactory();
        String hostName = parameters.get(RabbitMQConstants.SERVER_HOST_NAME);
        String portValue = parameters.get(RabbitMQConstants.SERVER_PORT);
        String serverRetryIntervalS = parameters.get(RabbitMQConstants.SERVER_RETRY_INTERVAL);
        String retryIntervalS = parameters.get(RabbitMQConstants.RETRY_INTERVAL);
        String retryCountS = parameters.get(RabbitMQConstants.RETRY_COUNT);
        String heartbeat = parameters.get(RabbitMQConstants.HEARTBEAT);
        String connectionTimeout = parameters.get(RabbitMQConstants.CONNECTION_TIMEOUT);
        String sslEnabledS = parameters.get(RabbitMQConstants.SSL_ENABLED);
        String userName = parameters.get(RabbitMQConstants.SERVER_USER_NAME);
        String password = parameters.get(RabbitMQConstants.SERVER_PASSWORD);
        String virtualHost = parameters.get(RabbitMQConstants.SERVER_VIRTUAL_HOST);
        String connectionPoolSizeS = parameters.get(RabbitMQConstants.CONNECTION_POOL_SIZE);

        if (!StringUtils.isEmpty(heartbeat)) {
            try {
                int heartbeatValue = Integer.parseInt(heartbeat);
                connectionFactory.setRequestedHeartbeat(heartbeatValue);
            } catch (NumberFormatException e) {
                //proceeding with rabbitmq default value
                log.warn("Number format error in reading heartbeat value. Proceeding with default");
            }
        }
        if (!StringUtils.isEmpty(connectionTimeout)) {
            try {
                int connectionTimeoutValue = Integer.parseInt(connectionTimeout);
                connectionFactory.setConnectionTimeout(connectionTimeoutValue);
            } catch (NumberFormatException e) {
                //proceeding with rabbitmq default value
                log.warn("Number format error in reading connection timeout value. Proceeding with default");
            }
        }

        if (!StringUtils.isEmpty(connectionPoolSizeS)) {
            try {
                connectionPoolSize = Integer.parseInt(connectionPoolSizeS);
            } catch (NumberFormatException e) {
                //proceeding with rabbitmq default value
                log.warn("Number format error in reading connection timeout value. Proceeding with default");
            }
        }

        if (!StringUtils.isEmpty(sslEnabledS)) {
            try {
                boolean sslEnabled = Boolean.parseBoolean(sslEnabledS);
                if (sslEnabled) {
                    String keyStoreLocation = parameters.get(RabbitMQConstants.SSL_KEYSTORE_LOCATION);
                    String keyStoreType = parameters.get(RabbitMQConstants.SSL_KEYSTORE_TYPE);
                    String keyStorePassword = parameters.get(RabbitMQConstants.SSL_KEYSTORE_PASSWORD);
                    String trustStoreLocation = parameters.get(RabbitMQConstants.SSL_TRUSTSTORE_LOCATION);
                    String trustStoreType = parameters.get(RabbitMQConstants.SSL_TRUSTSTORE_TYPE);
                    String trustStorePassword = parameters.get(RabbitMQConstants.SSL_TRUSTSTORE_PASSWORD);
                    String sslVersion = parameters.get(RabbitMQConstants.SSL_VERSION);

                    if (StringUtils.isEmpty(keyStoreLocation) || StringUtils.isEmpty(keyStoreType) || StringUtils.isEmpty(keyStorePassword) ||
                            StringUtils.isEmpty(trustStoreLocation) || StringUtils.isEmpty(trustStoreType) || StringUtils.isEmpty(trustStorePassword)) {
                        log.warn("Trustore and keystore information is not provided correctly. Proceeding with default SSL configuration");
                        connectionFactory.useSslProtocol();
                    } else {
                        char[] keyPassphrase = keyStorePassword.toCharArray();
                        KeyStore ks = KeyStore.getInstance(keyStoreType);
                        ks.load(new FileInputStream(keyStoreLocation), keyPassphrase);

                        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(ks, keyPassphrase);

                        char[] trustPassphrase = trustStorePassword.toCharArray();
                        KeyStore tks = KeyStore.getInstance(trustStoreType);
                        tks.load(new FileInputStream(trustStoreLocation), trustPassphrase);

                        TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
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

        if (!StringUtils.isEmpty(retryCountS)) {
            try {
                retryCount = Integer.parseInt(retryCountS);
            } catch (NumberFormatException e) {
                log.warn("Number format error in reading retry count value. Proceeding with default value (3)", e);
            }
        }

        if (!StringUtils.isEmpty(hostName)) {
            connectionFactory.setHost(hostName);
        } else {
            handleException("Host name is not defined");
        }

        try {
            int port = Integer.parseInt(portValue);
            if (port > 0) {
                connectionFactory.setPort(port);
            }
        } catch (NumberFormatException e) {
            handleException("Number format error in port number", e);
        }

        if (!StringUtils.isEmpty(userName)) {
            connectionFactory.setUsername(userName);
        }

        if (!StringUtils.isEmpty(password)) {
            connectionFactory.setPassword(password);
        }

        if (!StringUtils.isEmpty(virtualHost)) {
            connectionFactory.setVirtualHost(virtualHost);
        }

        if (!StringUtils.isEmpty(retryIntervalS)) {
            try {
                retryInterval = Integer.parseInt(retryIntervalS);
            } catch (NumberFormatException e) {
                log.warn("Number format error in reading retry interval value. Proceeding with default value (30000ms)", e);
            }
        }

        if (!StringUtils.isEmpty(serverRetryIntervalS)) {
            try {
                int serverRetryInterval = Integer.parseInt(serverRetryIntervalS);
                connectionFactory.setNetworkRecoveryInterval(serverRetryInterval);
            } catch (NumberFormatException e) {
                log.warn("Number format error in reading server retry interval value. Proceeding with default value", e);
            }
        }

        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setTopologyRecoveryEnabled(false);
    }

    public void initializeConnectionPool(boolean isDual) {
        if (isDual) {
            if (dualChannelPool == null) {
                log.info("Initializing dual channel pool of " + connectionPoolSize);
                dualChannelPool = new DualChannelPool(this, connectionPoolSize);
            }
        } else if (rmqChannelPool == null) {
            log.info("Initializing channel pool of " + connectionPoolSize);
            rmqChannelPool = new RMQChannelPool(this, connectionPoolSize);
        }
    }

    public DualChannel getRPCChannel() throws InterruptedException {
        return dualChannelPool.take();
    }

    public void pushRPCChannel(DualChannel dualChannel) throws InterruptedException {
        dualChannelPool.push(dualChannel);
    }

    public RMQChannel getRMQChannel() throws InterruptedException {
        return rmqChannelPool.take();
    }

    public void pushRMQChannel(RMQChannel channel) throws InterruptedException {
        rmqChannelPool.push(channel);
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Stop all the connections in this connection factory
     * Stop the executor service
     */
    public void stop() {
        es.shutdown();
    }

}

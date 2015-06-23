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
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.axis2.transport.rabbitmq.utils.AxisRabbitMQException;
import org.apache.axis2.transport.rabbitmq.utils.RabbitMQUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Encapsulate a RabbitMQ AMQP Connection factory definition within an Axis2.xml
 *
 * Connection Factory definitions, allows service level parameters to be defined,
 * and re-used by each service that binds to it
 *
 */
public class RabbitMQConnectionFactory {

    private static final Log log = LogFactory.getLog(RabbitMQConnectionFactory.class);

    private com.rabbitmq.client.ConnectionFactory connectionFactory = null;
    private String name;
    private Hashtable<String, String> parameters = new Hashtable<String, String>();
    private ExecutorService es = Executors.newFixedThreadPool(20);
    private Connection connection = null;
    private int retryInterval = 30000;
    private int retryCount = 3;


    public RabbitMQConnectionFactory(String name, com.rabbitmq.client.ConnectionFactory connectionFactory) {
        this.name = name;
        this.connectionFactory = connectionFactory;
    }

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
     * Create a rabbit mq connection
     * @return  a connection to the server
     */
    public Connection createConnection() throws IOException{
        Connection connection = null;
        try {
            connection = RabbitMQUtils.createConnection(connectionFactory);
            log.info("Successfully connected to RabbitMQ Broker");
        } catch (IOException e) {
            log.error("Error creating connection to RabbitMQ Broker. Reattempting to connect.");
            int retryC = 0;
            while ((connection == null) && ((retryCount == -1) || (retryC < retryCount))) {
                retryC++;
                log.info("Attempting to create connection to RabbitMQ Broker" +
                        " in " + retryInterval + " ms");
                try {
                    Thread.sleep(retryInterval);
                    connection = RabbitMQUtils.createConnection(connectionFactory);
                    log.info("Successfully connected to RabbitMQ Broker");
                } catch (InterruptedException e1) {
                    log.error("Error while trying to reconnect to RabbitMQ Broker", e1);
                } catch (IOException e2) {
                    log.error("Error while trying to reconnect to RabbitMQ Broker", e2);
                }
            }
            if (connection == null) {
                handleException("Could not connect to RabbitMQ Broker. Error while creating connection", e);
            } else {
                log.info("Successfully reconnected to RabbitMQ Broker");
            }
        }
        return connection;
    }

    /**
     * Create a connection from pool
     * @return  a connection to the server
     * @throws IOException
     */
    public Connection getConnectionPool() throws IOException {
        if (connection == null) {
            connection = connectionFactory.newConnection(es);
        }
        if(!connection.isOpen()){
            connection = connectionFactory.newConnection(es);
        }
        return connection;
    }

    /**
     * get connection factory name
     * @return connection factory name
     */
    public String getName() {
        return name;
    }

    /**
     * Set connection factory name
     * @param name name to set for the connection Factory
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Catch an exception an throw a AxisRabbitMQException with message
     * @param msg message to set for the exception
     * @param e throwable to set
     */
    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new AxisRabbitMQException(msg, e);
    }

    /**
     * Get all rabbit mq parameters
     * @return a map of parameters
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Initialize connection factory
     */
    private void initConnectionFactory() {
        connectionFactory = new com.rabbitmq.client.ConnectionFactory();
        String hostName = parameters.get(RabbitMQConstants.SERVER_HOST_NAME);
        String portValue = parameters.get(RabbitMQConstants.SERVER_PORT);
        String retryIntervalV = parameters.get(RabbitMQConstants.RETRY_INTERVAL);
        String retryCountV = parameters.get(RabbitMQConstants.RETRY_COUNT);
        String heartbeat = parameters.get(RabbitMQConstants.HEARTBEAT);
        String connectionTimeout = parameters.get(RabbitMQConstants.CONNECTION_TIMEOUT);

        if (heartbeat != null && !("").equals(heartbeat)) {
            try {
                int heartbeatValue = Integer.parseInt(heartbeat);
                connectionFactory.setRequestedHeartbeat(heartbeatValue);
            } catch (NumberFormatException e) {
                log.error("Number format error in reading heartbeat value. Proceeding with default");
            }
        }
        if (connectionTimeout != null && !("").equals(connectionTimeout)){
            try{
                int connectionTimeoutValue = Integer.parseInt(connectionTimeout);
                connectionFactory.setConnectionTimeout(connectionTimeoutValue);
            }catch (NumberFormatException e){
                log.error("Number format error in reading connection timeout value. Proceeding with default");
            }
        }

        if (retryIntervalV != null && !("").equals(retryIntervalV)) {
            try {
                retryInterval = Integer.parseInt(retryIntervalV);
            } catch (NumberFormatException e) {
                log.error("Number format error in reading retry interval value. Proceeding with default value (30000ms)");
            }
        }
        connectionFactory.setNetworkRecoveryInterval(retryInterval);
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setTopologyRecoveryEnabled(false);

        if (retryCountV != null && !("").equals(retryCountV)) {
            try {
                retryCount = Integer.parseInt(retryCountV);
            } catch (NumberFormatException e) {
                log.error("Number format error in reading retry count value. Proceeding with default value (3)");
            }
        }

        if (hostName != null && !hostName.equals("")) {
            connectionFactory.setHost(hostName);
        } else {
            throw new AxisRabbitMQException("Host name is not correctly defined");
        }
        int port = Integer.parseInt(portValue);
        if (port > 0) {
            connectionFactory.setPort(port);
        }
        String userName = parameters.get(RabbitMQConstants.SERVER_USER_NAME);

        if (userName != null && !userName.equals("")) {
            connectionFactory.setUsername(userName);
        }

        String password = parameters.get(RabbitMQConstants.SERVER_PASSWORD);

        if (password != null && !password.equals("")) {
            connectionFactory.setPassword(password);
        }
        String virtualHost = parameters.get(RabbitMQConstants.SERVER_VIRTUAL_HOST);

        if (virtualHost != null && !virtualHost.equals("")) {
            connectionFactory.setVirtualHost(virtualHost);
        }
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
    public void stop(){
        es.shutdown();
    }

}

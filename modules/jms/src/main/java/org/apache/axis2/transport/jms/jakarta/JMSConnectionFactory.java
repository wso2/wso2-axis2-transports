/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.jms.jakarta;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.transport.jms.AxisJMSException;
import org.apache.axis2.transport.jms.JMSConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.wso2.securevault.SecretResolver;
import org.wso2.securevault.SecureVaultException;
import org.wso2.securevault.commons.MiscellaneousUtil;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Encapsulate a JMS Connection factory definition within an Axis2.xml
 *
 * JMS Connection Factory definitions, allows JNDI properties as well as other service
 * level parameters to be defined, and re-used by each service that binds to it
 *
 * When used for sending messages out, the JMSConnectionFactory'ies are able to cache
 * a Connection, Session or Producer
 */
public class JMSConnectionFactory {

    private static final Log log = LogFactory.getLog(JMSConnectionFactory.class);

    /** The name used for the connection factory definition within Axis2 */
    private String name = null;
    /** The list of parameters from the axis2.xml definition */
    private Hashtable<String, String> parameters = new Hashtable<String, String>();

    /** The cached InitialContext reference */
    private Context context = null;
    /** The JMS ConnectionFactory this definition refers to */
    private ConnectionFactory conFactory = null;
    /** The shared JMS Connection for this JMS connection factory */
    private Connection sharedConnection = null;
    /** The Shared Destination */
    private Destination sharedDestination = null;
    /** The shared JMS connection for this JMS connection factory */
    private int cacheLevel = JMSConstants.CACHE_CONNECTION;

    private Map<Integer, Connection> sharedConnectionMap = new ConcurrentHashMap<>();
    private Map<Connection, SessionWrapper> sharedSessionWrapperMapPerConn = new ConcurrentHashMap<>();
    private Map<SessionWrapper, MessageProducer> sharedMessageProducerMap = new ConcurrentHashMap<>();

    public int getMaxSharedConnectionCount() {
        return maxSharedConnectionCount;
    }

    private int maxSharedConnectionCount = 10;
    private int lastReturnedConnectionIndex = 0;

    /**
     * Digest a JMS CF definition from an axis2.xml 'Parameter' and construct.
     * @param parameter the axis2.xml 'Parameter' that defined the JMS CF
     * @param secretResolver the SecretResolver to use to resolve secrets such as passwords
     */
    public JMSConnectionFactory(Parameter parameter, SecretResolver secretResolver) {
        this.name = parameter.getName();
        ParameterIncludeImpl pi = new ParameterIncludeImpl();

        try {
            pi.deserializeParameters((OMElement) parameter.getValue());
        } catch (AxisFault axisFault) {
            handleException("Error reading parameters for JMS connection factory "
                    + org.apache.axis2.transport.jms.JMSUtils.maskURLPasswordAndCredentials(name), axisFault);
        }

        for (Parameter param : pi.getParameters()) {
            OMElement paramElement = param.getParameterElement();
            String propertyValue = param.getValue().toString();
            if (paramElement != null) {
                if (secretResolver == null) {
                    throw new SecureVaultException("Cannot resolve secret password because axis2 secret resolver " +
                            "is null");
                }
                propertyValue = MiscellaneousUtil.resolve(paramElement, secretResolver);
            }
            parameters.put(param.getName(), propertyValue);
        }
        digestCacheLevel();
        initJMSConnectionFactory();
        setMaxSharedJMSConnectionsCount();
    }

    /**
     * Create a JMS CF definition from target endpoint reference
     *
     * @param targetEndpoint the JMS target address contains transport definitions
     */
    public JMSConnectionFactory(String targetEndpoint) {
        this.name = targetEndpoint;
        parameters.putAll(BaseUtils.getEPRProperties(targetEndpoint));
        digestCacheLevel();
        initJMSConnectionFactory();
        setMaxSharedJMSConnectionsCount();
    }

    /**
     * Initialize JMS connection factory based on transport parameters
     */
    private void initJMSConnectionFactory() {
        try {
            context = new InitialContext(parameters);
            conFactory = JMSUtils.lookup(context, ConnectionFactory.class,
                parameters.get(JMSConstants.PARAM_CONFAC_JNDI_NAME));
            if (parameters.get(JMSConstants.PARAM_DESTINATION) != null) {
                sharedDestination = JMSUtils.lookup(context, Destination.class,
                    parameters.get(JMSConstants.PARAM_DESTINATION));
            }
            log.info("JMS ConnectionFactory : " + JMSUtils.maskURLPasswordAndCredentials(name) + " initialized");

        } catch (NamingException e) {
            throw new AxisJMSException("Cannot acquire JNDI context, JMS Connection factory : "
                    + parameters.get(JMSConstants.PARAM_CONFAC_JNDI_NAME) + " or default destination : "
                    + parameters.get(JMSConstants.PARAM_DESTINATION) + " for JMS CF : "
                    + JMSUtils.maskURLPasswordAndCredentials(name) + " using : "
                    + JMSUtils.maskAxis2ConfigSensitiveParameters(parameters), e);
        }
    }

    /**
     * Digest, the cache value iff specified
     */
    private void digestCacheLevel() {

        String key = JMSConstants.PARAM_CACHE_LEVEL;
        String val = parameters.get(key);

        if ("none".equalsIgnoreCase(val)) {
            this.cacheLevel = JMSConstants.CACHE_NONE;
        } else if ("connection".equalsIgnoreCase(val)) {
            this.cacheLevel = JMSConstants.CACHE_CONNECTION;
        } else if ("session".equals(val)){
            this.cacheLevel = JMSConstants.CACHE_SESSION;
        } else if ("producer".equals(val)) {
            this.cacheLevel = JMSConstants.CACHE_PRODUCER;
        } else if ("consumer".equals(val)) {
            this.cacheLevel = JMSConstants.CACHE_CONSUMER;
        } else if (val != null) {
            throw new AxisJMSException("Invalid cache level : " + val + " for JMS CF : "
                    + JMSUtils.maskURLPasswordAndCredentials(name));
        }
    }

    private void setMaxSharedJMSConnectionsCount(){
        if(parameters.get(JMSConstants.MAX_JMS_CONNECTIONS_) != null){
            String maxConnectionCount = parameters.get(JMSConstants.MAX_JMS_CONNECTIONS_);
            try {
                int maxCount = Integer.parseInt(maxConnectionCount);
                if(maxCount > 0){
                    this.maxSharedConnectionCount = maxCount;
                    log.info("---- Max Shared JMS Connection Count Set to "+ maxSharedConnectionCount);
                }
            } catch (NumberFormatException e) {
                this.maxSharedConnectionCount = 10;
                log.error("Error in setting up the max shared jms connection count. Setting it to default value 10 ", e);
            }
        }
    }
    
    /**
     * Close all connections, sessions etc.. and stop this connection factory
     */
    public synchronized void stop() {
        if (sharedConnection != null) {
            try {
            	sharedConnection.close();
            } catch (JMSException e) {
                log.warn("Error shutting down connection factory : " + JMSUtils.maskURLPasswordAndCredentials(name), e);
            }
        }

        if (context != null) {
            try {
                context.close();
            } catch (NamingException e) {
                log.warn("Error while closing the InitialContext of factory : "
                        + JMSUtils.maskURLPasswordAndCredentials(name), e);
            }
        }
    }

    /**
     * Return the name assigned to this JMS CF definition
     * @return name of the JMS CF
     */
    public String getName() {
        return name;
    }

    /**
     * The list of properties (including JNDI and non-JNDI)
     * @return properties defined on the JMS CF
     */
    public Hashtable<String, String> getParameters() {
        return parameters;
    }

    /**
     * Get cached InitialContext
     * @return cache InitialContext
     */
    public Context getContext() {
        return context;
    }

    /**
     * Cache level applicable for this JMS CF
     * @return applicable cache level
     */
    public int getCacheLevel() {
        return cacheLevel;
    }

    /**
     * Get the shared Destination - if defined
     * @return
     */
    public Destination getSharedDestination() {
        return sharedDestination;
    }

    /**
     * Lookup a Destination using this JMS CF definitions and JNDI name
     * @param destinationName JNDI name of the Destionation
     * @param destinationType looking up destination type
     * @return JMS Destination for the given JNDI name or null
     */
    public Destination getDestination(String destinationName, String destinationType) {
        try {
            return JMSUtils.lookupDestination(context, destinationName, destinationType);
        } catch (NamingException e) {
            handleException("Error looking up the JMS destination with name " + destinationName
                    + " of type " + destinationType, e);
        }

        // never executes but keeps the compiler happy
        return null;
    }

    /**
     * Get the reply Destination from the PARAM_REPLY_DESTINATION parameter
     * @return reply destination defined in the JMS CF
     */
    public String getReplyToDestination() {
        return parameters.get(JMSConstants.PARAM_REPLY_DESTINATION);
    }

    /**
     * Get the reply destination type from the PARAM_REPLY_DEST_TYPE parameter
     * @return reply destination defined in the JMS CF
     */
    public String getReplyDestinationType() {
        return parameters.get(JMSConstants.PARAM_REPLY_DEST_TYPE) != null ?
                parameters.get(JMSConstants.PARAM_REPLY_DEST_TYPE) :
                JMSConstants.DESTINATION_TYPE_GENERIC;
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new AxisJMSException(msg, e);
    }

    /**
     * Should the JMS 1.1 API be used? - defaults to yes
     * @return true, if JMS 1.1 api should  be used
     */
    public boolean isJmsSpec11() {
        return parameters.get(JMSConstants.PARAM_JMS_SPEC_VER) == null ||
            JMSConstants.JMS_SPEC_VERSION_1_1.equals(parameters.get(JMSConstants.PARAM_JMS_SPEC_VER));
    }

    /**
     *  Should JMS 2.0 spec be used - default is false
     *  Added with JMS 2.0 update
     */
    public boolean isJmsSpec20() {
        return JMSConstants.JMS_SPEC_VERSION_2_0.equals(parameters.get(JMSConstants.PARAM_JMS_SPEC_VER));
    }

    /**
     *  JMS Spec. Version. This will be used in Transport Sender
     *  Added with JMS 2.0 update
     */
    public String jmsSpecVersion() {
        if (isJmsSpec11()) {
            return JMSConstants.JMS_SPEC_VERSION_1_1;
        } else if (isJmsSpec20()) {
            return JMSConstants.JMS_SPEC_VERSION_2_0;
        } else {
            return JMSConstants.JMS_SPEC_VERSION_1_0;
        }
    }

    /**
     * Return the type of the JMS CF Destination
     * @return TRUE if a Queue, FALSE for a Topic and NULL for a JMS 1.1 Generic Destination
     */
    public Boolean isQueue() {
        if (parameters.get(JMSConstants.PARAM_CONFAC_TYPE) == null &&
            parameters.get(JMSConstants.PARAM_DEST_TYPE) == null) {
            return null;
        }

        if (parameters.get(JMSConstants.PARAM_CONFAC_TYPE) != null) {
            if ("queue".equalsIgnoreCase(parameters.get(JMSConstants.PARAM_CONFAC_TYPE))) {
                return true;
            } else if ("topic".equalsIgnoreCase(parameters.get(JMSConstants.PARAM_CONFAC_TYPE))) {
                return false;
            } else {
                throw new AxisJMSException("Invalid " + JMSConstants.PARAM_CONFAC_TYPE + " : "
                        + parameters.get(JMSConstants.PARAM_CONFAC_TYPE) + " for JMS CF : "
                        + JMSUtils.maskURLPasswordAndCredentials(name));
            }
        } else {
            if ("queue".equalsIgnoreCase(parameters.get(JMSConstants.PARAM_DEST_TYPE))) {
                return true;
            } else if ("topic".equalsIgnoreCase(parameters.get(JMSConstants.PARAM_DEST_TYPE))) {
                return false;
            } else {
                throw new AxisJMSException("Invalid " + JMSConstants.PARAM_DEST_TYPE + " : "
                        + parameters.get(JMSConstants.PARAM_DEST_TYPE)
                        + " for JMS CF : " + JMSUtils.maskURLPasswordAndCredentials(name));
            }
        }
    }

    /**
     * Is a session transaction requested from users of this JMS CF?
     * @return session transaction required by the clients of this?
     */
    private boolean isSessionTransacted() {
        return parameters.get(JMSConstants.PARAM_SESSION_TRANSACTED) != null &&
            Boolean.valueOf(parameters.get(JMSConstants.PARAM_SESSION_TRANSACTED));
    }

    private boolean isDurable() {
        if (parameters.get(JMSConstants.PARAM_SUB_DURABLE) != null) {
            return Boolean.valueOf(parameters.get(JMSConstants.PARAM_SUB_DURABLE));
        }
        return false;
    }

    private boolean isSharedSubscription() {
        if (parameters.get(JMSConstants.PARAM_IS_SHARED_SUBSCRIPTION) != null) {
            return Boolean.valueOf(parameters.get(JMSConstants.PARAM_IS_SHARED_SUBSCRIPTION));
        }
        return false;
    }

    private String getClientId() {
        return parameters.get(JMSConstants.PARAM_DURABLE_SUB_CLIENT_ID);
    }

    /**
     * Create a new Connection
     * @return a new Connection
     */
    private Connection createConnection() {

        Connection connection = null;
        try {
            connection = JMSUtils.createConnection(
                conFactory,
                parameters.get(JMSConstants.PARAM_JMS_USERNAME),
                parameters.get(JMSConstants.PARAM_JMS_PASSWORD),
                jmsSpecVersion(), isQueue(), isDurable(), getClientId(), isSharedSubscription());

            if (log.isDebugEnabled()) {
                log.debug("New JMS Connection from JMS CF : " + JMSUtils.maskURLPasswordAndCredentials(name)
                        + " created");
            }

            connection.start();

        } catch (JMSException e) {
            handleException("Error acquiring a Connection from the JMS CF : " + JMSUtils.maskURLPasswordAndCredentials(name)
                    + " using properties : " + JMSUtils.maskAxis2ConfigSensitiveParameters(parameters), e);
        }
        return connection;
    }

    /**
     * Create a new SessionWrapper object for {@link Session}
     * @param connection Connection to use
     * @return A new Session
     */
    private SessionWrapper createSessionWrapper(Connection connection) throws JMSException {

            if (log.isDebugEnabled()) {
                log.debug("Creating a new JMS Session from JMS CF : " + JMSUtils.maskURLPasswordAndCredentials(name));
            }
            return new SessionWrapper(JMSUtils.createSession(
                    connection, isSessionTransacted(), Session.AUTO_ACKNOWLEDGE, jmsSpecVersion(), isQueue()));
    }

    /**
     * Create a new MessageProducer
     * @param session Session to be used
     * @param destination Destination to be used
     * @return a new MessageProducer
     */
    private MessageProducer createProducer(Session session, Destination destination) throws JMSException {
        if (log.isDebugEnabled()) {
            log.debug("Creating a new JMS MessageProducer from JMS CF : "
                      + JMSUtils.maskURLPasswordAndCredentials(name));
        }

        return JMSUtils.createProducer(session, destination, isQueue(), jmsSpecVersion());
    }

    /**
     * Get a new Connection or shared Connection from this JMS CF
     * @return new or shared Connection from this JMS CF
     */
    public Connection getConnection() {
        if (cacheLevel > JMSConstants.CACHE_NONE) {
            return getSharedConnection();
        } else {
            return createConnection();
        }
    }

    /**
     * Get a new Session or shared Session wrapper object from this JMS CF
     * @param connection the Connection to be used
     * @return new or shared Session from this JMS CF
     */
    public SessionWrapper getSessionWrapper(Connection connection) throws JMSException {
        if (cacheLevel > JMSConstants.CACHE_CONNECTION) {
            return getSharedSessionWrapper(connection);
        } else {
            return createSessionWrapper(connection);
        }
    }

    /**
     * Get a new Session or shared Session wrapper object from this JMS CF
     * @param connection the Connection to be used
     * @return new or shared Session from this JMS CF
     */
    public Session getSession(Connection connection) throws JMSException {
        return getSessionWrapper(connection).getSession();
    }

    /**
     * Get a new MessageProducer or shared MessageProducer from this JMS CF
     * @param sessionWrapper the Session wrapper to be used
     * @param destination the Destination to bind MessageProducer to
     * @return new or shared MessageProducer from this JMS CF
     */
    public MessageProducer getMessageProducer(SessionWrapper sessionWrapper, Destination destination) throws JMSException {
        if (cacheLevel > JMSConstants.CACHE_SESSION) {
            return getNullDestinationSharedProducer(sessionWrapper);
        } else {
            return createProducer(sessionWrapper.getSession(), destination);
        }
    }

    /**
     * Get a shared MessageProducer from this JMS CF with destination set to null to use with multiple destinations
     * when producer caching is enabled.
     *
     * @param sessionWrapper JMS session wrapper.
     * @return shared MessageProducer from this JMS CF with destination set to null
     */
    private synchronized MessageProducer getNullDestinationSharedProducer(SessionWrapper sessionWrapper)
            throws JMSException {
        MessageProducer messageProducer = sharedMessageProducerMap.get(sessionWrapper);
        if (messageProducer == null) {
            messageProducer = createProducer(sessionWrapper.getSession(), null);
            sharedMessageProducerMap.put(sessionWrapper, messageProducer);
            if (log.isDebugEnabled()) {
                log.debug("Created shared JMS MessageConsumer with no destination specified, for JMS CF : "
                        + JMSUtils.maskURLPasswordAndCredentials(name) + " , with producer caching enabled");
            }
        }
        return messageProducer;
    }


    /**
     * Get a new Connection or shared Connection from this JMS CF
     * @return new or shared Connection from this JMS CF
     */
    private synchronized Connection getSharedConnection() {

        Connection connection = sharedConnectionMap.get(lastReturnedConnectionIndex);
        if (connection == null) {
            connection = createConnection();
            setCorruptedConnectionListener(connection);
            sharedConnectionMap.put(lastReturnedConnectionIndex, connection);
        } else {
            try {
                String clientId = connection.getClientID();
                log.debug(" Returned connection with client Id: " + clientId);
            } catch (JMSException e) {
                // remove sessions/producers on the invalid connection
                removeInvalidSessionOfConnection(connection);
                // One scenario where the exception would be throw is for closed connections. This is more like a test
                // on borrow for connections
                if (log.isDebugEnabled()) {
                    log.debug("Resetting connection since test on borrow failed", e);
                }
                connection = createConnection();
                setCorruptedConnectionListener(connection);
                sharedConnectionMap.put(lastReturnedConnectionIndex, connection);
            }
        }
        lastReturnedConnectionIndex++;
        if (lastReturnedConnectionIndex >= maxSharedConnectionCount) {
            lastReturnedConnectionIndex = 0;
        }
        return connection;
    }

    /**
     * Get a SessionWrapper object which has {@link Session}
     *
     * @return shared SessionWrapper object
     */
    private synchronized SessionWrapper getSharedSessionWrapper(Connection connection) throws JMSException {
        SessionWrapper sessionWrapper = sharedSessionWrapperMapPerConn.get(connection);
        if (sessionWrapper == null) {
            sessionWrapper = createSessionWrapper(connection);
            sharedSessionWrapperMapPerConn.put(connection, sessionWrapper);
        } else {
            try {
                sessionWrapper.getSession().getTransacted();
            } catch (JMSException e) {
                // One scenario where the exception would be thrown is for closed sessions. This is more like a test
                // Test on borrow for sessions
                if (log.isDebugEnabled()) {
                    log.info("Resetting session since test on borrow failed", e);
                }
                removeInvalidSessionAndProducer(sessionWrapper);
                sessionWrapper = createSessionWrapper(connection);
                sharedSessionWrapperMapPerConn.put(connection, sessionWrapper);
            }
        }
        return sessionWrapper;
    }

    /**
     * Clear the troublesome connection from the map.
     *
     * @param connection Connection to clear
     */
    public synchronized void clearSharedConnection(Connection connection) {
        Iterator<Map.Entry<Integer, Connection>> connectionIterator = sharedConnectionMap.entrySet().iterator();
        while (connectionIterator.hasNext()) {
            Map.Entry<Integer, Connection> connectionEntry = connectionIterator.next();
            Connection jmsConnection = connectionEntry.getValue();
            if ((jmsConnection != null) && (jmsConnection.equals(connection))) {
                try {
                    log.warn("Closing corrupted connection - clientID: " + connection.getClientID());
                    connectionEntry.getValue().close();
                } catch (JMSException e) {
                    log.warn("Error while shutting down the connection : ", e);
                }
                connectionIterator.remove();
                removeInvalidSessionOfConnection(jmsConnection);
                break;
            }
        }
    }

    /**
     * Clear shared JMS objects and related connection
     * upon {@link JMSException} or {@link AxisJMSException}.
     *
     * @param connection troublesome connection
     */
    public void clearCache(Connection connection) {
        clearSharedConnection(connection);
    }

    /**
     * Sets exception listener for the shared JMS connection
     * to evict it upon an exception.
     *
     * @param jmsConnection JMS connection
     */
    private void setCorruptedConnectionListener(Connection jmsConnection) {
        try {
            jmsConnection.setExceptionListener(new CorruptedJMSConnectionListener(jmsConnection));
        } catch (JMSException e) {
            log.error("Error while setting ExceptionListener to the JMS connection", e);
        }
    }

    /**
     * Class defining custom exception listener to evict corrupted shared connections.
     */
    private class CorruptedJMSConnectionListener implements ExceptionListener {

        private final Connection jmsConnection;

        CorruptedJMSConnectionListener(Connection jmsConnection) {
            this.jmsConnection = jmsConnection;
        }

        @Override
        public void onException(JMSException e) {
            log.warn("An exception occurred on JMS connection. Evicting the cached connection", e);
            Iterator<Map.Entry<Integer, Connection>> connectionIterator = sharedConnectionMap.entrySet().iterator();
            while (connectionIterator.hasNext()) {
                Map.Entry<Integer, Connection> connectionEntry = connectionIterator.next();
                Connection connection = connectionEntry.getValue();
                if ((connection != null) && (connection.equals(jmsConnection))) {
                    clearCache(jmsConnection);
                    removeInvalidSessionOfConnection(jmsConnection);
                    connectionIterator.remove();
                    log.warn("Removing cached connection index: " + connectionEntry.getKey());
                    break;
                }
            }
        }
    }

    /**
     * Remove all invalid sessions once its connection is closed.
     * @param connection JMS connection.
     */
    private void removeInvalidSessionOfConnection(Connection connection) {
        SessionWrapper invalid = sharedSessionWrapperMapPerConn.remove(connection);
        try {
            invalid.getSession().close();
        } catch (JMSException e) {
            log.error("Error occurred while closing the session", e);
        }
        removeInvalidMessageProducer(invalid);
    }

    /**
     * Remove all invalid sessions once its connection is closed.
     *
     */
    public synchronized void removeInvalidSessionAndProducer(SessionWrapper sessionWrapper) {
        sharedSessionWrapperMapPerConn.entrySet().removeIf(entry -> entry.getValue().equals(sessionWrapper));
        try {
            sessionWrapper.getSession().close();
        } catch (JMSException e) {
            log.error("Error occurred while closing the session", e);
        }
        removeInvalidMessageProducer(sessionWrapper);
    }

    /**
     * Remove invalid message producers of the given session.
     * @param sessionWrapper session wrapper object.
     */
    private void removeInvalidMessageProducer(SessionWrapper sessionWrapper) {
        MessageProducer messageProducer = sharedMessageProducerMap.remove(sessionWrapper);
        try {
            messageProducer.close();
        } catch (JMSException e) {
            log.error("Error occurred while closing message producer", e);
        }
    }
}

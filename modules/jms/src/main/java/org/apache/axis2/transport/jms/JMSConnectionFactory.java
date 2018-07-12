/*
* Copyright 2004,2005 The Apache Software Foundation.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.axis2.transport.jms;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.securevault.SecretResolver;
import org.wso2.securevault.SecureVaultException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Hashtable;
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

    private Map<Integer, ConnectionDataHolder> sharedConnectionMap = new ConcurrentHashMap<>();
    private int maxSharedConnectionCount = 10;
    private int lastReturnedConnectionIndex = 0;

    /**
     * Digest a JMS CF definition from an axis2.xml 'Parameter' and construct.
     * @param parameter the axis2.xml 'Parameter' that defined the JMS CF
     */
    @Deprecated
    public JMSConnectionFactory(Parameter parameter) {

        this.name = parameter.getName();
        ParameterIncludeImpl pi = new ParameterIncludeImpl();

        try {
            pi.deserializeParameters((OMElement) parameter.getValue());
        } catch (AxisFault axisFault) {
            JMSUtils.handleException("Error reading parameters for JMS connection factory" + name, axisFault);
        }

        for (Object o : pi.getParameters()) {
            Parameter p = (Parameter) o;
            parameters.put(p.getName(), (String) p.getValue());
        }

        digestCacheLevel();
        try {
            context = new InitialContext(parameters);
            conFactory = JMSUtils.lookup(context, ConnectionFactory.class,
                    parameters.get(JMSConstants.PARAM_CONFAC_JNDI_NAME));
            if (parameters.get(JMSConstants.PARAM_DESTINATION) != null) {
                sharedDestination = JMSUtils.lookup(context, Destination.class,
                        parameters.get(JMSConstants.PARAM_DESTINATION));
            }
            log.info("JMS ConnectionFactory : " + name + " initialized");

        } catch (NamingException e) {
            throw new AxisJMSException("Cannot acquire JNDI context, JMS Connection factory : " +
                    parameters.get(JMSConstants.PARAM_CONFAC_JNDI_NAME) + " or default destination : " +
                    parameters.get(JMSConstants.PARAM_DESTINATION) +
                    " for JMS CF : " + name + " using : " + JMSUtils.maskAxis2ConfigSensitiveParameters(parameters), e);
        }
        setMaxSharedJMSConnectionsCount();
    }

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
            JMSUtils.handleException("Error reading parameters for JMS connection factory" + name, axisFault);
        }

        for (Parameter param : pi.getParameters()) {
            OMElement paramElement = param.getParameterElement();
            String propertyValue = param.getValue().toString();
            if (paramElement != null) {
                OMAttribute attribute = paramElement.getAttribute(JMSConstants.ALIAS_QNAME);
                if (attribute != null && attribute.getAttributeValue() != null
                        && !attribute.getAttributeValue().isEmpty()) {
                    if (secretResolver == null) {
                        throw new SecureVaultException("Axis2 Secret Resolver is null. "
                                + "Cannot resolve encrypted entry for " + param.getName());
                    }
                    if (secretResolver.isTokenProtected(attribute.getAttributeValue())) {
                        propertyValue = secretResolver.resolve(attribute.getAttributeValue());
                    }
                }
            }
            parameters.put(param.getName(), propertyValue);
        }
        digestCacheLevel();
        try {
            context = new InitialContext(parameters);
            conFactory = JMSUtils.lookup(context, ConnectionFactory.class,
                parameters.get(JMSConstants.PARAM_CONFAC_JNDI_NAME));
            if (parameters.get(JMSConstants.PARAM_DESTINATION) != null) {
                sharedDestination = JMSUtils.lookup(context, Destination.class,
                    parameters.get(JMSConstants.PARAM_DESTINATION));
            }
            log.info("JMS ConnectionFactory : " + name + " initialized");

        } catch (NamingException e) {
            throw new AxisJMSException("Cannot acquire JNDI context, JMS Connection factory : " +
                parameters.get(JMSConstants.PARAM_CONFAC_JNDI_NAME) + " or default destination : " +
                parameters.get(JMSConstants.PARAM_DESTINATION) +
                " for JMS CF : " + name + " using : " + JMSUtils.maskAxis2ConfigSensitiveParameters(parameters), e);
        }
        setMaxSharedJMSConnectionsCount();
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
            throw new AxisJMSException("Invalid cache level : " + val + " for JMS CF : " + name);
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
                log.warn("Error shutting down connection factory : " + name, e);
            }
        }

        clearSharedConnections();

        if (context != null) {
            try {
                context.close();
            } catch (NamingException e) {
                log.warn("Error while closing the InitialContext of factory : " + name, e);
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
    int getCacheLevel() {
        return cacheLevel;
    }

    /**
     * Get the shared Destination - if defined
     * @return shared JMS destination object
     */
    Destination getSharedDestination() {
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
            JMSUtils.handleException("Error looking up the JMS destination with name " + destinationName
                    + " of type " + destinationType, e);
        }

        // never executes but keeps the compiler happy
        return null;
    }

    ConnectionDataHolder getConnectionContainer() {
        if (cacheLevel > JMSConstants.CACHE_NONE) {
            return getSharedConnectionContainer();
        } else {
            return new ConnectionDataHolder(conFactory, this, 0);
        }
    }

    private synchronized ConnectionDataHolder getSharedConnectionContainer() {
        ConnectionDataHolder connectionDataHolder = sharedConnectionMap.get(lastReturnedConnectionIndex);

        if ((null == connectionDataHolder) && (sharedConnectionMap.size() <= maxSharedConnectionCount)) {
            connectionDataHolder = new ConnectionDataHolder(conFactory, this, lastReturnedConnectionIndex);
            sharedConnectionMap.put(lastReturnedConnectionIndex, connectionDataHolder);
        }
        lastReturnedConnectionIndex++;
        if (lastReturnedConnectionIndex >= maxSharedConnectionCount) {
            lastReturnedConnectionIndex = 0;
        }

        return connectionDataHolder;
    }

    /**
     * Clear the shared connection map due to stale connections
     */
    private synchronized void clearSharedConnections() {
        for (Map.Entry<Integer, ConnectionDataHolder> entry : sharedConnectionMap.entrySet()) {
            entry.getValue().close();
        }
        sharedConnectionMap.clear();
        lastReturnedConnectionIndex = 0;
    }

    /**
     * Get the reply destination type from the PARAM_REPLY_DEST_TYPE parameter
     * @return reply destination defined in the JMS CF
     */
    String getReplyDestinationType() {
        return parameters.get(JMSConstants.PARAM_REPLY_DEST_TYPE) != null ?
                parameters.get(JMSConstants.PARAM_REPLY_DEST_TYPE) : JMSConstants.DESTINATION_TYPE_GENERIC;
    }

    /**
     *  JMS Spec. Version. This will be used in Transport Sender
     *  Added with JMS 2.0 update
     */
    String jmsSpecVersion() {
        return JMSUtils.jmsSpecVersion(parameters);
    }

    /**
     * Return the type of the JMS CF Destination
     * @return TRUE if a Queue, FALSE for a Topic and NULL for a JMS 1.1 Generic Destination
     */
    Boolean isQueue() {
        return JMSUtils.isQueue(parameters, name);
    }

    /**
     * In case of a broker failure, remove cached connections in order to attempt new connections.
     * @param connectionIndex index of connection container
     */
    public void clearCachedConnection(int connectionIndex) {
        ConnectionDataHolder connectionDataHolder = sharedConnectionMap.get(connectionIndex);

        if (null != connectionDataHolder) {
            connectionDataHolder.close();
            sharedConnectionMap.remove(connectionIndex);
        }
    }

}

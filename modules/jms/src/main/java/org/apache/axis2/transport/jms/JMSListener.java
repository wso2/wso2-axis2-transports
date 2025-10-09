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

import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.transport.base.AbstractTransportListenerEx;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.ManagementSupport;
import org.apache.axis2.transport.base.event.TransportErrorListener;
import org.apache.axis2.transport.base.event.TransportErrorSource;
import org.apache.axis2.transport.base.event.TransportErrorSourceSupport;
import org.wso2.securevault.SecretResolver;

import java.util.Hashtable;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * The revamped JMS Transport listener implementation. Creates {@link ServiceTaskManager} instances
 * for each service requesting exposure over JMS, and stops these if they are undeployed / stopped.
 * <p>
 * A service indicates a JMS Connection factory definition by name, which would be defined in the
 * JMSListner on the axis2.xml, and this provides a way to reuse common configuration between
 * services, as well as to optimize resources utilized
 * <p>
 * If the connection factory name was not specified, it will default to the one named "default"
 * {@see JMSConstants.DEFAULT_CONFAC_NAME}
 * <p>
 * If a destination JNDI name is not specified, a service will expect to use a Queue with the same
 * JNDI name as of the service. Additional Parameters allows one to bind to a Topic or specify
 * many more detailed control options. See package documentation for more details
 * <p>
 * All Destinations / JMS Administered objects used MUST be pre-created or already available 
 */
public class JMSListener extends AbstractTransportListenerEx<JMSEndpoint> implements ManagementSupport,
    TransportErrorSource {

    public static final String TRANSPORT_NAME = Constants.TRANSPORT_JMS;

    /** The JMSConnectionFactoryManager which centralizes the management of defined factories */
    private JMSConnectionFactoryManager connFacManager;

    private final TransportErrorSourceSupport tess = new TransportErrorSourceSupport(this);

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doInit() throws AxisFault {
        SecretResolver secretResolver = getConfigurationContext().getAxisConfiguration().getSecretResolver();
        connFacManager = new JMSConnectionFactoryManager(getTransportInDescription(), secretResolver);
        log.info("JMS Transport Receiver/Listener initialized...");
    }

    @Override
    protected JMSEndpoint createEndpoint() {
        return new JMSEndpoint(this, workerPool);
    }

    /**
     * Listen for JMS messages on behalf of the given service
     *
     * @param endpoint the jms endpoint which is connected with the service
     */
    @Override
    protected void startEndpoint(JMSEndpoint endpoint) throws AxisFault {
        if (endpoint.isJMSSpec31()) {
            org.apache.axis2.transport.jms.jakarta.ServiceTaskManager stm = endpoint.getJakartaServiceTaskManager();
            boolean connected = false;

            /* The following parameters are used when trying to connect with the JMS provider at the start up*/
            int r = 1;
            long retryDuration = 10000;
            double reconnectionProgressionFactor = 2.0;
            long maxReconnectDuration = stm.getMaxReconnectDuration(); // default 1 min
            Hashtable<String, String> jmsProperties = stm.getJmsProperties();

            // First we will check whether jms provider is started or not, as if not it will throw a continuous error log
            // If jms provider not started we will wait for exponentially increasing time intervals, till the provider is started
            while (!connected) {
                boolean jmsProviderStarted = checkJMSJakartaConnection(stm);
                if (jmsProviderStarted) {
                    log.info("Connection attempt: " + r + " for JMS Provider for service: " +
                            stm.getServiceName() + " was successful!");
                    connected = true;
                    stm.start();

                    for (int i = 0; i < 3; i++) {
                        // Check the consumer count rather than the active task count. Reason: if the
                        // destination is of type topic, then the transport is only ready to receive
                        // messages if at least one consumer exists. This is of not much importance,
                        // except for automated tests.
                        if (stm.getConsumerCount() > 0) {
                            log.info("Started to listen on destination : " + stm.getDestinationJNDIName() +
                                    " of type " + org.apache.axis2.transport.jms.jakarta.JMSUtils.
                                    getDestinationTypeAsString(stm.getDestinationType()) +
                                    " for service " + stm.getServiceName());
                            return;
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignore) {
                        }
                    }

                    log.warn("Polling tasks on destination : " + stm.getDestinationJNDIName() +
                            " of type " + org.apache.axis2.transport.jms.jakarta.JMSUtils.
                            getDestinationTypeAsString(stm.getDestinationType()) +
                            " for service " + stm.getServiceName() + " have not yet started after 3 seconds ..");
                } else {
                    log.error("Unable to continue server startup as it seems the JMS Provider is not yet started. Please start the JMS provider now.");
                    retryDuration = (long) (retryDuration * reconnectionProgressionFactor);
                    log.error("Connection attempt : " + (r++) + " for JMS Provider failed. Next retry in " + (retryDuration / 1000) + " seconds");
                    if (retryDuration > maxReconnectDuration) {
                        retryDuration = maxReconnectDuration;
                        stm.start();
                        break;
                    }
                    try {
                        Thread.sleep(retryDuration);
                    } catch (InterruptedException ignore) {
                    }
                }
            }
        } else {
            ServiceTaskManager stm = endpoint.getServiceTaskManager();
            boolean connected = false;

            /* The following parameters are used when trying to connect with the JMS provider at the start up*/
            int r = 1;
            long retryDuration = 10000;
            double reconnectionProgressionFactor = 2.0;
            long maxReconnectDuration = stm.getMaxReconnectDuration(); // default 1 min
            Hashtable<String, String> jmsProperties = stm.getJmsProperties();

            // First we will check whether jms provider is started or not, as if not it will throw a continuous error log
            // If jms provider not started we will wait for exponentially increasing time intervals, till the provider is started
            while (!connected) {
                boolean jmsProviderStarted = checkJMSConnection(stm);
                if (jmsProviderStarted) {
                    log.info("Connection attempt: " + r + " for JMS Provider for service: " + stm.getServiceName() + " was successful!");
                    connected = true;
                    stm.start();

                    for (int i = 0; i < 3; i++) {
                        // Check the consumer count rather than the active task count. Reason: if the
                        // destination is of type topic, then the transport is only ready to receive
                        // messages if at least one consumer exists. This is of not much importance,
                        // except for automated tests.
                        if (stm.getConsumerCount() > 0) {
                            log.info("Started to listen on destination : " + stm.getDestinationJNDIName() +
                                    " of type " + JMSUtils.getDestinationTypeAsString(stm.getDestinationType()) +
                                    " for service " + stm.getServiceName());
                            return;
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignore) {
                        }
                    }

                    log.warn("Polling tasks on destination : " + stm.getDestinationJNDIName() +
                            " of type " + JMSUtils.getDestinationTypeAsString(stm.getDestinationType()) +
                            " for service " + stm.getServiceName() + " have not yet started after 3 seconds ..");
                } else {
                    log.error("Unable to continue server startup as it seems the JMS Provider is not yet started. Please start the JMS provider now.");
                    retryDuration = (long) (retryDuration * reconnectionProgressionFactor);
                    log.error("Connection attempt : " + (r++) + " for JMS Provider failed. Next retry in " + (retryDuration / 1000) + " seconds");
                    if (retryDuration > maxReconnectDuration) {
                        retryDuration = maxReconnectDuration;
                        stm.start();
                        break;
                    }
                    try {
                        Thread.sleep(retryDuration);
                    } catch (InterruptedException ignore) {
                    }
                }
            }
        }
    }

    private boolean checkJMSConnection(ServiceTaskManager stm) {

        Connection connection = null;
        Hashtable<String, String> jmsProperties = stm.getJmsProperties();
        try {
            ConnectionFactory jmsConFactory = null;
            try {
                jmsConFactory = JMSUtils.lookup(
                        new InitialContext(stm.getJmsProperties()), ConnectionFactory.class, stm.getConnFactoryJNDIName());
            } catch (NamingException e) {
                log.error("Error looking up connection factory : " + stm.getConnFactoryJNDIName() +
                        "using JNDI properties : " + JMSUtils.maskAxis2ConfigSensitiveParameters(jmsProperties), e);
            }
            connection = JMSUtils.createConnection(
                    jmsConFactory,
                    jmsProperties.get(JMSConstants.PARAM_JMS_USERNAME),
                    jmsProperties.get(JMSConstants.PARAM_JMS_PASSWORD), stm.getJmsSpec(), null, false, "", false);
        } catch (JMSException ignore){
            Throwable innerException = ignore.getLinkedException();
            if(innerException != null){
                log.warn("Error connecting to JMS.", innerException);    
            }else{
                log.warn("Error connecting to JMS.", ignore);
            }            
        }

        // Get the connection status
        boolean connectionStatus = (connection != null);

        try {
            //Closing the opened jms connection
            if (connectionStatus) {
                log.debug("Closing the opened Jms connection");
                connection.close();
            }
        } catch (JMSException e) {
            log.error("Error Closing JMS Connection", e);
        }

        return connectionStatus;
    }

    private boolean checkJMSJakartaConnection(org.apache.axis2.transport.jms.jakarta.ServiceTaskManager stm) {

        jakarta.jms.Connection connection = null;
        Hashtable<String, String> jmsProperties = stm.getJmsProperties();
        try {
            jakarta.jms.ConnectionFactory jmsConFactory = null;
            try {
                jmsConFactory = org.apache.axis2.transport.jms.jakarta.JMSUtils.lookup(
                        new InitialContext(stm.getJmsProperties()), jakarta.jms.ConnectionFactory.class,
                        stm.getConnFactoryJNDIName());
            } catch (NamingException e) {
                log.error("Error looking up connection factory : " + stm.getConnFactoryJNDIName() +
                        "using JNDI properties : " + org.apache.axis2.transport.jms.jakarta.JMSUtils.
                        maskAxis2ConfigSensitiveParameters(jmsProperties), e);
            }
            connection = org.apache.axis2.transport.jms.jakarta.JMSUtils.createConnection(
                    jmsConFactory,
                    jmsProperties.get(JMSConstants.PARAM_JMS_USERNAME),
                    jmsProperties.get(JMSConstants.PARAM_JMS_PASSWORD), stm.getJmsSpec(), null,
                    false, "", false);
        } catch (jakarta.jms.JMSException ignore){
            Throwable innerException = ignore.getLinkedException();
            if(innerException != null){
                log.warn("Error connecting to JMS.", innerException);
            }else{
                log.warn("Error connecting to JMS.", ignore);
            }
        }

        // Get the connection status
        boolean connectionStatus = (connection != null);

        try {
            //Closing the opened jms connection
            if (connectionStatus) {
                log.debug("Closing the opened Jms connection");
                connection.close();
            }
        } catch (jakarta.jms.JMSException e) {
            log.error("Error Closing JMS Connection", e);
        }

        return connectionStatus;
    }

    /**
     * Stops listening for messages for the service thats undeployed or stopped
     *
     * @param endpoint the jms endpoint which is connected with the service
     */
    @Override
    protected void stopEndpoint(JMSEndpoint endpoint) {
        boolean listenerShuttingDown = (state == BaseConstants.STOPPED);
        if (endpoint.isJMSSpec31()) {
            org.apache.axis2.transport.jms.jakarta.ServiceTaskManager stm = endpoint.getJakartaServiceTaskManager();
            if (log.isDebugEnabled()) {
                log.debug("Stopping listening on destination : " + stm.getDestinationJNDIName() +
                        " for service : " + stm.getServiceName());
            }

            stm.stop(listenerShuttingDown);

            log.info("Stopped listening for JMS messages to service : " + endpoint.getServiceName());
        } else {
            ServiceTaskManager stm = endpoint.getServiceTaskManager();
            if (log.isDebugEnabled()) {
                log.debug("Stopping listening on destination : " + stm.getDestinationJNDIName() +
                        " for service : " + stm.getServiceName());
            }

            stm.stop(listenerShuttingDown);

            log.info("Stopped listening for JMS messages to service : " + endpoint.getServiceName());
        }
    }

    /**
     * Return the connection factory name for this service. If this service
     * refers to an invalid factory or defaults to a non-existent default
     * factory, this returns null
     *
     * @param service the AxisService
     * @return the JMSConnectionFactory to be used, or null if reference is invalid
     */
    public JMSConnectionFactory getConnectionFactory(AxisService service) {

        Parameter conFacParam = service.getParameter(JMSConstants.PARAM_JMS_CONFAC);
        // validate connection factory name (specified or default)
        if (conFacParam != null) {
            return connFacManager.getJMSConnectionFactory((String) conFacParam.getValue());
        } else {
            return connFacManager.getJMSConnectionFactory(JMSConstants.DEFAULT_CONFAC_NAME);
        }
    }

    /**
     * Return the connection factory name for this service. If this service
     * refers to an invalid factory or defaults to a non-existent default
     * factory, this returns null
     *
     * @param service the AxisService
     * @return the JMSConnectionFactory to be used, or null if reference is invalid
     */
    public org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory getJakartaConnectionFactory(AxisService service) {

        Parameter conFacParam = service.getParameter(JMSConstants.PARAM_JMS_CONFAC);
        // validate connection factory name (specified or default)
        if (conFacParam != null) {
            return connFacManager.getJakartaConnectionFactory((String) conFacParam.getValue());
        } else {
            return connFacManager.getJakartaConnectionFactory(JMSConstants.DEFAULT_CONFAC_NAME);
        }
    }

    // -- jmx/management methods--
    /**
     * Pause the listener - Stop accepting/processing new messages, but continues processing existing
     * messages until they complete. This helps bring an instance into a maintenence mode
     * @throws AxisFault on error
     */
    @Override
    public void pause() throws AxisFault {
        if (state != BaseConstants.STARTED) return;
        try {
            for (JMSEndpoint endpoint : getEndpoints()) {
                if (endpoint.isJMSSpec31()) {
                    endpoint.getJakartaServiceTaskManager().pause();
                } else {
                    endpoint.getServiceTaskManager().pause();
                }
            }
            state = BaseConstants.PAUSED;
            log.info("Listener paused");
        } catch (AxisJMSException e) {
            log.error("At least one service could not be paused", e);
        }
    }

    /**
     * Resume the lister - Brings the lister into active mode back from a paused state
     * @throws AxisFault on error
     */
    @Override
    public void resume() throws AxisFault {
        if (state != BaseConstants.PAUSED) return;
        try {
            state = BaseConstants.STARTED;
            for (JMSEndpoint endpoint : getEndpoints()) {
                if (endpoint.isJMSSpec31()) {
                    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                    endpoint.getJakartaServiceTaskManager().scheduleNewTaskIfAppropriate();
                    endpoint.getJakartaServiceTaskManager().resume();
                } else {
                    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                    endpoint.getServiceTaskManager().scheduleNewTaskIfAppropriate();
                    endpoint.getServiceTaskManager().resume();
                }
            }
            log.info("Listener resumed");
        } catch (AxisJMSException e) {
            log.error("At least one service could not be resumed", e);
        }
    }

    /**
     * Stop processing new messages, and wait the specified maximum time for in-flight
     * requests to complete before a controlled shutdown for maintenence
     *
     * @param millis a number of milliseconds to wait until pending requests are allowed to complete
     * @throws AxisFault on error
     */
    @Override
    public void maintenenceShutdown(long millis) throws AxisFault {
        if (state != BaseConstants.STARTED) return;
        try {
            long start = System.currentTimeMillis();
            stop();
            state = BaseConstants.STOPPED;
            log.info("Listener shutdown in : " + (System.currentTimeMillis() - start) / 1000 + "s");
        } catch (Exception e) {
            handleException("Error shutting down the listener for maintenence", e);
        }
    }

    public void addErrorListener(TransportErrorListener listener) {
        tess.addErrorListener(listener);
    }

    public void removeErrorListener(TransportErrorListener listener) {
        tess.removeErrorListener(listener);
    }

    void error(AxisService service, Throwable ex) {
        tess.error(service, ex);
    }
    
    public void clearActiveConnections() {
    	log.error("Not Implemented.");
    }

    public int getState() {
        return this.state;
    }
}

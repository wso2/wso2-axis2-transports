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

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.securevault.SecretResolver;
import org.wso2.securevault.SecureVaultException;
import org.wso2.securevault.commons.MiscellaneousUtil;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import javax.naming.Context;

/**
 * Class managing a set of {@link JMSConnectionFactory} objects.
 */
public class JMSConnectionFactoryManager {

    private static final Log log = LogFactory.getLog(JMSConnectionFactoryManager.class);

    /** A Map containing the JMS connection factories managed by this, keyed by name */
    private final Map<String,JMSConnectionFactory> connectionFactories =
        new HashMap<String,JMSConnectionFactory>();

    /** A Map containing the JMS connection factories managed by this, keyed by name */
    private final Map<String, org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory> jakartaConnectionFactories =
            new HashMap<String, org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory>();

    /**
     * Construct a Connection factory manager for the JMS transport sender or receiver
     *
     * @param trpInDesc the transport description for JMS
     */
    @Deprecated
    public JMSConnectionFactoryManager(ParameterInclude trpInDesc) {
        loadConnectionFactoryDefinitions(trpInDesc);
    }

    /**
     * Create JMSConnectionFactory instances for the definitions in the transport configuration,
     * and add these into our collection of connectionFactories map keyed by name
     *
     * @param trpDesc the transport description for JMS
     */
    @Deprecated
    private void loadConnectionFactoryDefinitions(ParameterInclude trpDesc) {

        for (Parameter p : trpDesc.getParameters()) {
            try {
                JMSConnectionFactory jmsConFactory = new JMSConnectionFactory(p);
                connectionFactories.put(jmsConFactory.getName(), jmsConFactory);
            } catch (AxisJMSException e) {
                log.error("Error setting up connection factory : " + p.getName(), e);
            }
        }
    }

    /**
     * Construct a Connection factory manager for the JMS transport sender or receiver.
     *
     * @param trpInDesc the transport description for JMS
     * @param secretResolver the SecretResolver to use to resolve secrets such as passwords
     */
    public JMSConnectionFactoryManager(ParameterInclude trpInDesc, SecretResolver secretResolver) {
        loadConnectionFactoryDefinitions(trpInDesc, secretResolver);
    }

    /**
     * Create JMSConnectionFactory instances for the definitions in the transport configuration,
     * and add these into our collection of connectionFactories map keyed by name.
     *
     * @param trpDesc the transport description for JMS
     * @param secretResolver the SecretResolver to use to resolve secrets such as passwords
     */
    private void loadConnectionFactoryDefinitions(ParameterInclude trpDesc, SecretResolver secretResolver) {
        for (Parameter parameter : trpDesc.getParameters()) {
            try {
                boolean isJMSSpec31 = JMSUtils.isJmsSpec31(parameter);
                if (isJMSSpec31) {
                    org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory jmsConFactory = new org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory(parameter, secretResolver);
                    jakartaConnectionFactories.put(jmsConFactory.getName(), jmsConFactory);
                } else {
                    JMSConnectionFactory jmsConFactory = new JMSConnectionFactory(parameter, secretResolver);
                    connectionFactories.put(jmsConFactory.getName(), jmsConFactory);
                }
            } catch (AxisJMSException e) {
                log.error("Error setting up connection factory : " + parameter.getName(), e);
            }
        }
    }

    /**
     * Create JMSConnectionFactory instance from the definitions in the target address,
     * and add into connectionFactories map keyed by the target address
     *
     * @param targetEndpoint the JMS target address contains transport definitions
     */
    public JMSConnectionFactory getConnectionFactoryFromTargetEndpoint(String targetEndpoint) {
        try {
            if (!connectionFactories.containsKey(targetEndpoint)) {
                JMSConnectionFactory jmsConnectionFactory = new JMSConnectionFactory(targetEndpoint);
                connectionFactories.put(jmsConnectionFactory.getName(), jmsConnectionFactory);
            }
        } catch (AxisJMSException e) {
            log.error("Error setting up connection factory : " + targetEndpoint, e);
        }
        return connectionFactories.get(targetEndpoint);
    }

    /**
     * Create JMSConnectionFactory instance from the definitions in the target address,
     * and add into connectionFactories map keyed by the target address
     *
     * @param targetEndpoint the JMS target address contains transport definitions
     */
    public org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory getJakartaConnectionFactoryFromTargetEndpoint(
            String targetEndpoint) {
        try {
            if (!jakartaConnectionFactories.containsKey(targetEndpoint)) {
                org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory jmsConnectionFactory =
                        new org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory(targetEndpoint);
                jakartaConnectionFactories.put(jmsConnectionFactory.getName(), jmsConnectionFactory);
            }
        } catch (AxisJMSException e) {
            log.error("Error setting up connection factory : " + targetEndpoint, e);
        }
        return jakartaConnectionFactories.get(targetEndpoint);
    }

    /**
     * Get the JMS connection factory with the given name.
     *
     * @param name the name of the JMS connection factory
     * @return the JMS connection factory or null if no connection factory with
     *         the given name exists
     */
    public JMSConnectionFactory getJMSConnectionFactory(String name) {
        return connectionFactories.get(name);
    }

    /**
     * Get the JMS connection factory with the given name.
     *
     * @param name the name of the JMS connection factory
     * @return the JMS connection factory or null if no connection factory with
     *         the given name exists
     */
    public org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory getJakartaConnectionFactory(String name) {
        return jakartaConnectionFactories.get(name);
    }

    /**
     * Get the JMS connection factory that matches the given properties, i.e. referring to
     * the same underlying connection factory. Used by the JMSSender to determine if already
     * available resources should be used for outgoing messages
     *
     * @param props a Map of connection factory JNDI properties and name
     * @return the JMS connection factory or null if no connection factory compatible
     *         with the given properties exists
     */
    public JMSConnectionFactory getJMSConnectionFactory(Map<String,String> props) {
        for (JMSConnectionFactory cf : connectionFactories.values()) {
            Map<String,String> cfProperties = cf.getParameters();
            
            if (equals(props.get(JMSConstants.PARAM_CONFAC_JNDI_NAME),
                cfProperties.get(JMSConstants.PARAM_CONFAC_JNDI_NAME))
                &&
                equals(props.get(Context.INITIAL_CONTEXT_FACTORY),
                    cfProperties.get(Context.INITIAL_CONTEXT_FACTORY))
                &&
                equals(props.get(Context.PROVIDER_URL),
                    cfProperties.get(Context.PROVIDER_URL))
                &&
                equals(props.get(JMSConstants.PARAM_CACHE_LEVEL),
                    cfProperties.get(JMSConstants.PARAM_CACHE_LEVEL))
                &&
                equals(props.get(JMSConstants.PARAM_SESSION_TRANSACTED),
                    cfProperties.get(JMSConstants.PARAM_SESSION_TRANSACTED))
                &&
                equals(props.get(Context.SECURITY_PRINCIPAL),
                    cfProperties.get(Context.SECURITY_PRINCIPAL))
                &&
                equals(props.get(Context.SECURITY_CREDENTIALS),
                    cfProperties.get(Context.SECURITY_CREDENTIALS))
                &&
                equals(props.get(JMSConstants.PARAM_JMS_USERNAME),
                    cfProperties.get(JMSConstants.PARAM_JMS_USERNAME))
                &&
                equals(props.get(JMSConstants.PARAM_JMS_PASSWORD),
                    cfProperties.get(JMSConstants.PARAM_JMS_PASSWORD))) {
                return cf;
            }
        }
        return null;
    }

    /**
     * Get the JMS connection factory that matches the given properties, i.e. referring to
     * the same underlying connection factory. Used by the JMSSender to determine if already
     * available resources should be used for outgoing messages
     *
     * @param props a Map of connection factory JNDI properties and name
     * @return the JMS connection factory or null if no connection factory compatible
     *         with the given properties exists
     */
    public org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory getJakartaConnectionFactory(Map<String,String> props) {
        for (org.apache.axis2.transport.jms.jakarta.JMSConnectionFactory cf : jakartaConnectionFactories.values()) {
            Map<String,String> cfProperties = cf.getParameters();

            if (equals(props.get(JMSConstants.PARAM_CONFAC_JNDI_NAME),
                    cfProperties.get(JMSConstants.PARAM_CONFAC_JNDI_NAME))
                    &&
                    equals(props.get(Context.INITIAL_CONTEXT_FACTORY),
                            cfProperties.get(Context.INITIAL_CONTEXT_FACTORY))
                    &&
                    equals(props.get(Context.PROVIDER_URL),
                            cfProperties.get(Context.PROVIDER_URL))
                    &&
                    equals(props.get(JMSConstants.PARAM_CACHE_LEVEL),
                            cfProperties.get(JMSConstants.PARAM_CACHE_LEVEL))
                    &&
                    equals(props.get(JMSConstants.PARAM_SESSION_TRANSACTED),
                            cfProperties.get(JMSConstants.PARAM_SESSION_TRANSACTED))
                    &&
                    equals(props.get(Context.SECURITY_PRINCIPAL),
                            cfProperties.get(Context.SECURITY_PRINCIPAL))
                    &&
                    equals(props.get(Context.SECURITY_CREDENTIALS),
                            cfProperties.get(Context.SECURITY_CREDENTIALS))
                    &&
                    equals(props.get(JMSConstants.PARAM_JMS_USERNAME),
                            cfProperties.get(JMSConstants.PARAM_JMS_USERNAME))
                    &&
                    equals(props.get(JMSConstants.PARAM_JMS_PASSWORD),
                            cfProperties.get(JMSConstants.PARAM_JMS_PASSWORD))) {
                return cf;
            }
        }
        return null;
    }

    /**
     * Compare two values preventing NPEs
     */
    private static boolean equals(Object s1, Object s2) {
        return s1 == s2 || s1 != null && s1.equals(s2);
    }
    
    /**
     * Stop all connection factories.
     */
    public void stop() {
        for (JMSConnectionFactory conFac : connectionFactories.values()) {
            conFac.stop();
        }
    }

    protected void handleException(String msg, Exception e) throws AxisFault {
        log.error(msg, e);
        throw new AxisFault(msg, e);
    }

    private Hashtable<String, String> resolveParameters(Parameter parameter, SecretResolver secretResolver) throws AxisFault {
        Hashtable<String, String> parameters = new Hashtable<String, String>();
        String name = parameter.getName();
        ParameterIncludeImpl pi = new ParameterIncludeImpl();

        try {
            pi.deserializeParameters((OMElement) parameter.getValue());
        } catch (AxisFault axisFault) {
            handleException("Error reading parameters for JMS connection factory "
                    + JMSUtils.maskURLPasswordAndCredentials(name), axisFault);
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
        return parameters;
    }
}

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
package org.apache.axis2.transport.oracleaq;

import org.apache.axis2.AxisFault;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.ParamUtils;
import org.apache.axis2.transport.base.ProtocolEndpoint;
import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.axis2.transport.oracleaq.ctype.ContentTypeRuleFactory;
import org.apache.axis2.transport.oracleaq.ctype.ContentTypeRuleSet;
import org.apache.axis2.transport.oracleaq.ctype.MessageTypeRule;
import org.apache.axis2.transport.oracleaq.ctype.PropertyRule;
import org.apache.axis2.addressing.EndpointReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.naming.Context;

/**
 * Class that links an Axis2 service to a JMS destination. Additionally, it contains
 * all the required information to process incoming JMS messages and to inject them
 * into Axis2.
 */
public class OracleAQEndpoint extends ProtocolEndpoint {
    private static final Log log = LogFactory.getLog(OracleAQEndpoint.class);
    
    private final OracleAQListener listener;
    private final WorkerPool workerPool;
    
    private OracleAQConnectionFactory cf;
    private String jndiDestinationName;
    private int destinationType = OracleAQConstants.GENERIC;
    private String jndiReplyDestinationName;
    private String replyDestinationType = OracleAQConstants.DESTINATION_TYPE_GENERIC;
    private Set<EndpointReference> endpointReferences = new HashSet<EndpointReference>();
    private ContentTypeRuleSet contentTypeRuleSet;
    private ServiceTaskManager serviceTaskManager;
    private String hyphenSupport = OracleAQConstants.DEFAULT_HYPHEN_SUPPORT;

    public OracleAQEndpoint(OracleAQListener listener, WorkerPool workerPool) {
        this.listener = listener;
        this.workerPool = workerPool;
    }

    public String getJndiDestinationName() {
        return jndiDestinationName;
    }

    private void setDestinationType(String destinationType) {
        if (OracleAQConstants.DESTINATION_TYPE_TOPIC.equalsIgnoreCase(destinationType)) {
            this.destinationType = OracleAQConstants.TOPIC;
        } else if (OracleAQConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(destinationType)) {
            this.destinationType = OracleAQConstants.QUEUE;
        } else {
            this.destinationType = OracleAQConstants.GENERIC;
        }
    }

    private void setReplyDestinationType(String destinationType) {
        if (OracleAQConstants.DESTINATION_TYPE_TOPIC.equalsIgnoreCase(destinationType)) {
            this.replyDestinationType = OracleAQConstants.DESTINATION_TYPE_TOPIC;
        } else if (OracleAQConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(destinationType)) {
            this.replyDestinationType = OracleAQConstants.DESTINATION_TYPE_QUEUE;
        } else {
            this.replyDestinationType = OracleAQConstants.DESTINATION_TYPE_GENERIC;
        }
    }

    public String getJndiReplyDestinationName() {
        return jndiReplyDestinationName;
    }

    public String getReplyDestinationType() {
        return replyDestinationType;
    }

    @Override
    public EndpointReference[] getEndpointReferences(AxisService service, String ip) {
        return endpointReferences.toArray(new EndpointReference[endpointReferences.size()]);
    }

    private void computeEPRs() {
        List<EndpointReference> eprs = new ArrayList<EndpointReference>();
        for (Object o : getService().getParameters()) {
            Parameter p = (Parameter) o;
            if (OracleAQConstants.PARAM_PUBLISH_EPR.equals(p.getName()) && p.getValue() instanceof String) {
                if ("legacy".equalsIgnoreCase((String) p.getValue())) {
                    // if "legacy" specified, compute and replace it
                    endpointReferences.add(
                        new EndpointReference(getEPR()));
                } else {
                    endpointReferences.add(new EndpointReference((String) p.getValue()));
                }
            }
        }

        if (eprs.isEmpty()) {
            // if nothing specified, compute and return legacy EPR
            endpointReferences.add(new EndpointReference(getEPR()));
        }
    }

    /**
     * Get the EPR for the given JMS connection factory and destination
     * the form of the URL is
     * oracleaq:/<destination>?[<key>=<value>&]*
     * Credentials Context.SECURITY_PRINCIPAL, Context.SECURITY_CREDENTIALS
     * OracleAQConstants.PARAM_JMS_USERNAME and OracleAQConstants.PARAM_JMS_USERNAME are filtered
     *
     * @return the EPR as a String
     */
    private String getEPR() {
        StringBuffer sb = new StringBuffer();

        sb.append(
            OracleAQConstants.JMS_PREFIX).append(jndiDestinationName);
        sb.append("?").
            append(OracleAQConstants.PARAM_DEST_TYPE).append("=").append(
            destinationType == OracleAQConstants.TOPIC ?
                OracleAQConstants.DESTINATION_TYPE_TOPIC : OracleAQConstants.DESTINATION_TYPE_QUEUE);

        if (contentTypeRuleSet != null) {
            String contentTypeProperty = contentTypeRuleSet.getDefaultContentTypeProperty();
            if (contentTypeProperty != null) {
                sb.append("&");
                sb.append(OracleAQConstants.CONTENT_TYPE_PROPERTY_PARAM);
                sb.append("=");
                sb.append(contentTypeProperty);
            }
        }

        for (Map.Entry<String,String> entry : cf.getParameters().entrySet()) {
            if (!Context.SECURITY_PRINCIPAL.equalsIgnoreCase(entry.getKey()) &&
                !Context.SECURITY_CREDENTIALS.equalsIgnoreCase(entry.getKey()) &&
                !OracleAQConstants.PARAM_JMS_USERNAME.equalsIgnoreCase(entry.getKey()) &&
                !OracleAQConstants.PARAM_JMS_PASSWORD.equalsIgnoreCase(entry.getKey())) {
                sb.append("&").append(
                    entry.getKey()).append("=").append(entry.getValue());
            }
        }
        return sb.toString();
    }

    public ContentTypeRuleSet getContentTypeRuleSet() {
        return contentTypeRuleSet;
    }

    public OracleAQConnectionFactory getCf() {
        return cf;
    }

    public ServiceTaskManager getServiceTaskManager() {
        return serviceTaskManager;
    }

    public void setServiceTaskManager(ServiceTaskManager serviceTaskManager) {
        this.serviceTaskManager = serviceTaskManager;
    }

    public String getHyphenSupport() {
        return hyphenSupport;
    }

    @Override
    public boolean loadConfiguration(ParameterInclude params) throws AxisFault {
        // We only support endpoints configured at service level
        if (!(params instanceof AxisService)) {
            return false;
        }
        
        AxisService service = (AxisService)params;
        
        cf = listener.getConnectionFactory(service);
        if (cf == null) {
            return false;
        }

        Parameter destParam = service.getParameter(OracleAQConstants.PARAM_DESTINATION);
        if (destParam != null) {
            jndiDestinationName = (String)destParam.getValue();
        } else {
            // Assume that the JNDI destination name is the same as the service name
            jndiDestinationName = service.getName();
        }
        
        Parameter destTypeParam = service.getParameter(OracleAQConstants.PARAM_DEST_TYPE);
        if (destTypeParam != null) {
            String paramValue = (String) destTypeParam.getValue();
            if (OracleAQConstants.DESTINATION_TYPE_QUEUE.equals(paramValue) ||
                    OracleAQConstants.DESTINATION_TYPE_TOPIC.equals(paramValue) )  {
                setDestinationType(paramValue);
            } else {
                throw new AxisFault("Invalid destinaton type value " + paramValue);
            }
        } else {
            log.debug("JMS destination type not given. default queue");
            destinationType = OracleAQConstants.QUEUE;
        }

        Parameter replyDestTypeParam = service.getParameter(OracleAQConstants.PARAM_REPLY_DEST_TYPE);
        if (replyDestTypeParam != null) {
            String paramValue = (String) replyDestTypeParam.getValue();
            if (OracleAQConstants.DESTINATION_TYPE_QUEUE.equals(paramValue) ||
                    OracleAQConstants.DESTINATION_TYPE_TOPIC.equals(paramValue) )  {
                setReplyDestinationType(paramValue);
            } else {
                throw new AxisFault("Invalid destination type value " + paramValue);
            }
        } else {
            log.debug("JMS reply destination type not given. default queue");
            replyDestinationType = OracleAQConstants.DESTINATION_TYPE_QUEUE;
        }
        
        jndiReplyDestinationName = ParamUtils.getOptionalParam(service,
                OracleAQConstants.PARAM_REPLY_DESTINATION);
        
        Parameter contentTypeParam = service.getParameter(OracleAQConstants.CONTENT_TYPE_PARAM);
        if (contentTypeParam == null) {
            contentTypeRuleSet = new ContentTypeRuleSet();
            contentTypeRuleSet.addRule(new PropertyRule(BaseConstants.CONTENT_TYPE));
            contentTypeRuleSet.addRule(new MessageTypeRule(BytesMessage.class, "application/octet-stream"));
            contentTypeRuleSet.addRule(new MessageTypeRule(TextMessage.class, "text/plain"));
        } else {
            contentTypeRuleSet = ContentTypeRuleFactory.parse(contentTypeParam);
        }

        computeEPRs(); // compute service EPR and keep for later use        
        
        serviceTaskManager = ServiceTaskManagerFactory.createTaskManagerForService(cf, service, workerPool);
        serviceTaskManager.setOracleAQMessageReceiver(new OracleAQMessageReceiver(listener, cf, this));

        // Fix for ESBJAVA-3687, retrieve JMS transport property transport.oracleaq.MessagePropertyHyphens and set this
        // into the msgCtx
        Parameter paramHyphenSupport = service.getParameter(OracleAQConstants.PARAM_JMS_HYPHEN_MODE);
        if (paramHyphenSupport != null) {
            if (((String) paramHyphenSupport.getValue()).equals(OracleAQConstants.HYPHEN_MODE_REPLACE)) {
                hyphenSupport = OracleAQConstants.HYPHEN_MODE_REPLACE;
            } else if (((String) paramHyphenSupport.getValue()).equals(OracleAQConstants.HYPHEN_MODE_DELETE)) {
                hyphenSupport = OracleAQConstants.HYPHEN_MODE_DELETE;
            }
        }

        return true;
    }
}

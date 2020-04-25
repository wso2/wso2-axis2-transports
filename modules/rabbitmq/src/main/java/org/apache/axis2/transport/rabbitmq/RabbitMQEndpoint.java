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
import org.apache.axis2.addressing.EndpointReference;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.transport.base.ProtocolEndpoint;
import org.apache.axis2.transport.base.threads.WorkerPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class that links an Axis2 service to a RabbitMQ AMQP destination. Additionally, it contains all the required
 * information to process incoming AMQP messages and to inject them into Axis2.
 */
public class RabbitMQEndpoint extends ProtocolEndpoint {

    private final WorkerPool workerPool;
    private final RabbitMQListener rabbitMQListener;
    private Set<EndpointReference> endpointReferences = new HashSet<>();
    private ServiceTaskManager serviceTaskManager;

    /**
     * Create a rabbit mq endpoint
     *
     * @param rabbitMQListener listener listening for messages from this EP
     * @param workerPool       worker pool to create service task manager
     */
    public RabbitMQEndpoint(RabbitMQListener rabbitMQListener, WorkerPool workerPool) {
        this.rabbitMQListener = rabbitMQListener;
        this.workerPool = workerPool;
    }

    @Override
    public EndpointReference[] getEndpointReferences(AxisService service, String ip) {
        return endpointReferences.toArray(new EndpointReference[0]);
    }

    /**
     * Get service task manager for this EP
     *
     * @return service task manager set for this EP
     */
    public ServiceTaskManager getServiceTaskManager() {
        return serviceTaskManager;
    }

    /**
     * Set service task manager for the EP
     *
     * @param serviceTaskManager service task manager to be set
     */
    public void setServiceTaskManager(ServiceTaskManager serviceTaskManager) {
        this.serviceTaskManager = serviceTaskManager;
    }

    @Override
    public boolean loadConfiguration(ParameterInclude params) throws AxisFault {
        // We only support endpoints configured at service level
        if (!(params instanceof AxisService)) {
            return false;
        }

        AxisService service = (AxisService) params;
        Parameter conFacParam = service.getParameter(RabbitMQConstants.RABBITMQ_CON_FAC);
        String factoryName;
        if (conFacParam != null) {
            factoryName = (String) conFacParam.getValue();
        } else {
            return false;
        }
        serviceTaskManager = createServiceTaskManager(factoryName, service, workerPool);
        serviceTaskManager.setRabbitMQMessageReceiver(new RabbitMQMessageReceiver(rabbitMQListener, this));

        return true;
    }

    /**
     * Create a ServiceTaskManager for the service passed in and its corresponding ConnectionFactory
     *
     * @param factoryName The connection factory name
     * @param service     the AxisService instance to send the ServiceTaskManager
     * @param workerPool  to be used with ServiceTaskManager
     * @return ServiceTaskManager
     */
    private ServiceTaskManager createServiceTaskManager(String factoryName, AxisService service, WorkerPool workerPool) {

        RabbitMQConnectionFactory rabbitMQConnectionFactory = rabbitMQListener.getRabbitMQConnectionFactory();
        RabbitMQConnectionPool rabbitMQConnectionPool = rabbitMQListener.getRabbitMQConnectionPool();
        String serviceName = service.getName();
        Map<String, String> serviceParameters = getServiceStringParameters(service.getParameters());
        Map<String, String> cfParameters = rabbitMQConnectionFactory.getConnectionFactoryConfiguration(factoryName);
        ServiceTaskManager taskManager = new ServiceTaskManager(rabbitMQConnectionPool, factoryName);

        taskManager.setServiceName(serviceName);
        taskManager.addRabbitMQProperties(cfParameters);
        taskManager.addRabbitMQProperties(serviceParameters);

        taskManager.setWorkerPool(workerPool);

        return taskManager;
    }

    /**
     * Extract parameters from the AxisService parameters
     *
     * @param list AxisService parameter list
     * @return map of service parameters
     */
    private Map<String, String> getServiceStringParameters(List<Parameter> list) {
        Map<String, String> map = new HashMap<>();
        for (Parameter p : list) {
            if (p.getValue() instanceof String) {
                map.put(p.getName(), (String) p.getValue());
            }
        }
        return map;
    }
}

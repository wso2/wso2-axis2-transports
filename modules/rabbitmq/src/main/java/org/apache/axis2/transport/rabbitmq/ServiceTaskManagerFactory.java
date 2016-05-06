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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.transport.base.threads.WorkerPool;

/**
 * Class that manages the creation of ServiceTaskManager for a AxisService and
 * ConnectionFactory
 */
public class ServiceTaskManagerFactory {

	/**
	 * The number of concurrent consumers to be created to poll for messages for
	 * this service For Topics, this should be ONE, to prevent receipt of
	 * multiple copies of the same message
	 */
	public static final String PARAM_CONCURRENT_CONSUMERS = "rabbitmq.server.ConcurrentConsumers";

	/**
	 * Create a ServiceTaskManager for the service passed in and its
	 * corresponding ConnectionFactory
	 *
	 * @param rabbitMQConnectionFactory
	 *            the ConnectionFactory instance to used with ServiceTaskManager
	 * @param service
	 *            the AxisService instance to send the ServiceTaskManager
	 * @param workerPool
	 *            to be used with ServiceTaskManager
	 * @return ServiceTaskManager
	 */
	public static ServiceTaskManager createTaskManagerForService(RabbitMQConnectionFactory rabbitMQConnectionFactory,
			AxisService service, WorkerPool workerPool) {

		String serviceName = service.getName();
		Map<String, String> serviceParameters = getServiceStringParameters(service.getParameters());
		Map<String, String> cfParameters = rabbitMQConnectionFactory.getParameters();

		ServiceTaskManager taskManager = new ServiceTaskManager(rabbitMQConnectionFactory);

		taskManager.setServiceName(serviceName);
		taskManager.addRabbitMQProperties(cfParameters);
		taskManager.addRabbitMQProperties(serviceParameters);

		taskManager.setWorkerPool(workerPool);

		Integer value = getOptionalIntProperty(PARAM_CONCURRENT_CONSUMERS, serviceParameters, cfParameters);
		if (value != null) {
			taskManager.setConcurrentConsumers(value);
		}

		return taskManager;
	}

	private static Map<String, String> getServiceStringParameters(List<Parameter> list) {

		Map<String, String> map = new HashMap<String, String>();
		for (Parameter p : list) {
			if (p.getValue() instanceof String) {
				map.put(p.getName(), (String) p.getValue());
			}
		}
		return map;
	}

	private static Integer getOptionalIntProperty(String key, Map<String, String> svcMap, Map<String, String> cfMap) {

		String value = svcMap.get(key);
		if (value == null) {
			value = cfMap.get(key);
		}
		if (value == null) {
			return null;
		} else {
			try {
				return Integer.parseInt(value);
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value : " + value + " for " + key);
			}
		}
	}

}

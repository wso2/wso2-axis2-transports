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

import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.threads.WorkerPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jms.Session;

public class ServiceTaskManagerFactory {
    private ServiceTaskManagerFactory() {}

    /**
     * Create a ServiceTaskManager for the service passed in and its corresponding JMSConnectionFactory
     * @param jcf
     * @param service
     * @param workerPool
     * @return
     */
    public static ServiceTaskManager createTaskManagerForService(JMSConnectionFactory jcf,
        AxisService service, WorkerPool workerPool) {

        String name = service.getName();
        Map<String, String> svc = getServiceStringParameters(service.getParameters());
        Map<String, String> cf  = jcf.getParameters();

        ServiceTaskManager stm = new ServiceTaskManager();

        stm.setServiceName(name);
        stm.addJmsProperties(cf);
        stm.addJmsProperties(svc);

        stm.setConnFactoryJNDIName(
            getRqdStringProperty(JMSConstants.PARAM_CONFAC_JNDI_NAME, svc, cf));
		stm.setReconnectDuration(getOptionalLongProperty(JMSConstants.PARAM_RECONNECT_INTERVAL,
		                                                 svc, cf));
        stm.setConsumeErrorRetryDelay(getOptionalIntProperty(JMSConstants.PARAM_CONSUME_ERROR_DELAY, svc, cf));
        stm.setConsumeErrorProgressionFactor(getOptionalDoubleProperty(
                JMSConstants.PARAM_CONSUME_ERROR_PROGRESSION, svc, cf));
        stm.setMaxConsumeErrorRetryBeforeDelay(getOptionalIntProperty(
                JMSConstants.PARAM_MAX_CONSUME_RETRY_BEFORE_DELAY, svc, cf));

        String destName = getOptionalStringProperty(JMSConstants.PARAM_DESTINATION, svc, cf);
        if (destName == null) {
            destName = service.getName();
        }
        stm.setDestinationJNDIName(destName);
        stm.setDestinationType(getDestinationType(svc, cf));
        if(getOptionalBooleanProperty(JMSConstants.PARAM_SUB_DURABLE, svc, cf) != null &&
                getOptionalBooleanProperty(JMSConstants.PARAM_SUB_DURABLE, svc, cf)){
            stm.setDurableSubscriberClientId(getRqdStringProperty(
                JMSConstants.PARAM_DURABLE_SUB_CLIENT_ID, svc, cf));
        }

        stm.setJmsSpec(
            getJMSSpecVersion(svc, cf));
        stm.setTransactionality(
            getTransactionality(svc, cf));
        stm.setCacheUserTransaction(
            getOptionalBooleanProperty(BaseConstants.PARAM_CACHE_USER_TXN, svc, cf));
        stm.setUserTransactionJNDIName(
            getOptionalStringProperty(BaseConstants.PARAM_USER_TXN_JNDI_NAME, svc, cf));
        stm.setSessionTransacted(
            getOptionalBooleanProperty(JMSConstants.PARAM_SESSION_TRANSACTED, svc, cf));
        stm.setSessionAckMode(
            getSessionAck(svc, cf));
        stm.setMessageSelector(
            getOptionalStringProperty(JMSConstants.PARAM_MSG_SELECTOR, svc, cf));
        stm.setSubscriptionDurable(
            getOptionalBooleanProperty(JMSConstants.PARAM_SUB_DURABLE, svc, cf));
        stm.setDurableSubscriberName(
            getOptionalStringProperty(JMSConstants.PARAM_DURABLE_SUB_NAME, svc, cf));
        stm.setSharedSubscription(
                getOptionalBooleanProperty(JMSConstants.PARAM_IS_SHARED_SUBSCRIPTION, svc, cf));

        stm.setCacheLevel(
            getCacheLevel(svc, cf));
        stm.setPubSubNoLocal(
            getOptionalBooleanProperty(JMSConstants.PARAM_PUBSUB_NO_LOCAL, svc, cf));

        Integer value = getOptionalIntProperty(JMSConstants.PARAM_RCV_TIMEOUT, svc, cf);
        if (value != null) {
            stm.setReceiveTimeout(value);
        }
        value = getOptionalIntProperty(JMSConstants.PARAM_CONCURRENT_CONSUMERS, svc, cf);
        if (value != null) {
            stm.setConcurrentConsumers(value);
        }
        value = getOptionalIntProperty(JMSConstants.PARAM_MAX_CONSUMERS, svc, cf);
        if (value != null) {
            stm.setMaxConcurrentConsumers(value);
        }
        value = getOptionalIntProperty(JMSConstants.PARAM_IDLE_TASK_LIMIT, svc, cf);
        if (value != null) {
            stm.setIdleTaskExecutionLimit(value);
        }
        value = getOptionalIntProperty(JMSConstants.PARAM_MAX_MSGS_PER_TASK, svc, cf);
        if (value != null) {
            stm.setMaxMessagesPerTask(value);
        }

        value = getOptionalIntProperty(JMSConstants.PARAM_RECON_INIT_DURATION, svc, cf);
        if (value != null) {
            stm.setInitialReconnectDuration(value);
        }
        value = getOptionalIntProperty(JMSConstants.PARAM_RECON_MAX_DURATION, svc, cf);
        if (value != null) {
            stm.setMaxReconnectDuration(value);
        }
        Double dValue = getOptionalDoubleProperty(JMSConstants.PARAM_RECON_FACTOR, svc, cf);
        if (dValue != null) {
            stm.setReconnectionProgressionFactor(dValue);
        }

        stm.setWorkerPool(workerPool);

        // remove processed properties from property bag
        stm.removeJmsProperties(JMSConstants.PARAM_CONFAC_JNDI_NAME);
        stm.removeJmsProperties(JMSConstants.PARAM_DESTINATION);
        stm.removeJmsProperties(JMSConstants.PARAM_JMS_SPEC_VER);
        stm.removeJmsProperties(BaseConstants.PARAM_TRANSACTIONALITY);
        stm.removeJmsProperties(BaseConstants.PARAM_CACHE_USER_TXN);
        stm.removeJmsProperties(BaseConstants.PARAM_USER_TXN_JNDI_NAME);
        stm.removeJmsProperties(JMSConstants.PARAM_SESSION_TRANSACTED);
        stm.removeJmsProperties(JMSConstants.PARAM_MSG_SELECTOR);
        stm.removeJmsProperties(JMSConstants.PARAM_SUB_DURABLE);
        stm.removeJmsProperties(JMSConstants.PARAM_DURABLE_SUB_NAME);
        stm.removeJmsProperties(JMSConstants.PARAM_CACHE_LEVEL);
        stm.removeJmsProperties(JMSConstants.PARAM_PUBSUB_NO_LOCAL);
        stm.removeJmsProperties(JMSConstants.PARAM_RCV_TIMEOUT);
        stm.removeJmsProperties(JMSConstants.PARAM_CONCURRENT_CONSUMERS);
        stm.removeJmsProperties(JMSConstants.PARAM_MAX_CONSUMERS);
        stm.removeJmsProperties(JMSConstants.PARAM_IDLE_TASK_LIMIT);
        stm.removeJmsProperties(JMSConstants.PARAM_MAX_MSGS_PER_TASK);
        stm.removeJmsProperties(JMSConstants.PARAM_RECON_INIT_DURATION);
        stm.removeJmsProperties(JMSConstants.PARAM_RECON_MAX_DURATION);
        stm.removeJmsProperties(JMSConstants.PARAM_RECON_FACTOR);
        stm.removeJmsProperties(JMSConstants.PARAM_DURABLE_SUB_CLIENT_ID);

        return stm;
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

    private static String getRqdStringProperty(String key, Map<String,String> svcMap,
            Map<String,String> cfMap) {
        
        String value = svcMap.get(key);
        if (value == null) {
            value = cfMap.get(key);
        }
        if (value == null) {
            throw new AxisJMSException("Service/connection factory property : " + key);
        }
        return value;
    }

    private static String getOptionalStringProperty(String key, Map<String,String> svcMap,
            Map<String,String> cfMap) {
        
        String value = svcMap.get(key);
        if (value == null) {
            value = cfMap.get(key);
        }
        return value;
    }

    private static Boolean getOptionalBooleanProperty(String key, Map<String,String> svcMap,
            Map<String,String> cfMap) {
        
        String value = svcMap.get(key);
        if (value == null) {
            value = cfMap.get(key);
        }
        if (value == null) {
            return null;
        } else {
            return Boolean.valueOf(value);
        }
    }
    
	private static Long getOptionalLongProperty(String key, Map<String, String> svcMap,
	                                            Map<String, String> cfMap) {

		String value = svcMap.get(key);
		if (value == null) {
			value = cfMap.get(key);
		}
		if (value == null) {
			return null;
		} else {
			try {
				return Long.parseLong(value);
			} catch (NumberFormatException e) {
				throw new AxisJMSException("Invalid value : " + value + " for " + key);
			}
		}
	}   

    private static Integer getOptionalIntProperty(String key, Map<String,String> svcMap,
            Map<String,String> cfMap) {
        
        String value = svcMap.get(key);
        if (value == null) {
            value = cfMap.get(key);
        }
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new AxisJMSException("Invalid value : " + value + " for " + key);
            }
        }
        return null;
    }

    private static Double getOptionalDoubleProperty(String key, Map<String,String> svcMap,
            Map<String,String> cfMap) {
        
        String value = svcMap.get(key);
        if (value == null) {
            value = cfMap.get(key);
        }
        if (value != null) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new AxisJMSException("Invalid value : " + value + " for " + key);
            }
        }
        return null;
    }

    private static int getTransactionality(Map<String,String> svcMap, Map<String,String> cfMap) {

        String key = BaseConstants.PARAM_TRANSACTIONALITY;
        String val = svcMap.get(key);
        if (val == null) {
            val = cfMap.get(key);
        }

        if (val == null) {
            return BaseConstants.TRANSACTION_NONE;

        } else {    
            if (BaseConstants.STR_TRANSACTION_JTA.equalsIgnoreCase(val)) {
                return BaseConstants.TRANSACTION_JTA;
            } else if (BaseConstants.STR_TRANSACTION_LOCAL.equalsIgnoreCase(val)) {
                return BaseConstants.TRANSACTION_LOCAL;
            } else {
                throw new AxisJMSException("Invalid option : " + val + " for parameter : " +
                    BaseConstants.STR_TRANSACTION_JTA);
            }
        }
    }

    private static int getDestinationType(Map<String,String> svcMap, Map<String,String> cfMap) {

        String key = JMSConstants.PARAM_DEST_TYPE;
        String val = svcMap.get(key);
        if (val == null) {
            val = cfMap.get(key);
        }

        if (JMSConstants.DESTINATION_TYPE_TOPIC.equalsIgnoreCase(val)) {
            return JMSConstants.TOPIC;
        }
        return JMSConstants.QUEUE;
    }

    private static int getSessionAck(Map<String,String> svcMap, Map<String,String> cfMap) {

        String key = JMSConstants.PARAM_SESSION_ACK;
        String val = svcMap.get(key);
        if (val == null) {
            val = cfMap.get(key);
        }

        if (val == null || "AUTO_ACKNOWLEDGE".equalsIgnoreCase(val)) {
            return Session.AUTO_ACKNOWLEDGE;
        } else if ("CLIENT_ACKNOWLEDGE".equalsIgnoreCase(val)) {
            return Session.CLIENT_ACKNOWLEDGE;
        } else if ("DUPS_OK_ACKNOWLEDGE".equals(val)){
            return Session.DUPS_OK_ACKNOWLEDGE;
        } else if ("SESSION_TRANSACTED".equals(val)) {
            return 0; //Session.SESSION_TRANSACTED;
        } else {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException ignore) {
                throw new AxisJMSException("Invalid session acknowledgement mode : " + val);
            }
        }
    }

    private static int getCacheLevel(Map<String,String> svcMap, Map<String,String> cfMap) {

        String key = JMSConstants.PARAM_CACHE_LEVEL;
        String val = svcMap.get(key);
        if (val == null) {
            val = cfMap.get(key);
        }

        if ("none".equalsIgnoreCase(val)) {
            return JMSConstants.CACHE_NONE;
        } else if ("connection".equalsIgnoreCase(val)) {
            return JMSConstants.CACHE_CONNECTION;
        } else if ("session".equals(val)){
            return JMSConstants.CACHE_SESSION;
        } else if ("consumer".equals(val)) {
            return JMSConstants.CACHE_CONSUMER;
        } else if ("producer".equals(val)) {
            return JMSConstants.CACHE_PRODUCER;
        } else if (val != null) {
            throw new AxisJMSException("Invalid cache level : " + val);
        }
        return JMSConstants.CACHE_AUTO;
    }

    private static String getJMSSpecVersion(Map<String, String> svcMap, Map<String, String> cfMap) {

        String key = JMSConstants.PARAM_JMS_SPEC_VER;
        String val = svcMap.get(key);
        if (val == null) {
            val = cfMap.get(key);
        }

        if (val == null || JMSConstants.JMS_SPEC_VERSION_1_1.equals(val)) {
            return JMSConstants.JMS_SPEC_VERSION_1_1;
        } else if (JMSConstants.JMS_SPEC_VERSION_2_0.equals(val)) {
            return JMSConstants.JMS_SPEC_VERSION_2_0;
        } else {
            return JMSConstants.JMS_SPEC_VERSION_1_0;
        }
    }
}

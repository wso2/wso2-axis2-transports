/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.jms.jakarta;

import junit.framework.TestCase;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.transport.jms.AxisJMSException;
import org.apache.axis2.transport.jms.JMSConstants;
import org.mockito.Mockito;

import java.util.Hashtable;

/**
 * Unit tests for the Jakarta JMS {@link ServiceTaskManagerFactory}, covering how the reconnect and throttle
 * parameters are read from the service and connection factory configuration.
 */
public class ServiceTaskManagerFactoryTest extends TestCase {

    private static final String SERVICE_NAME = "TestService";
    private static final long DEFAULT_MAX_RECONNECT_DURATION = 60000L;
    private static final int DEFAULT_THROTTLE_LIMIT_PER_MIN = 60;

    /**
     * Max reconnect duration set at service level, with no throttle limit configured, must be applied
     * instead of failing with a NullPointerException.
     */
    public void testMaxReconnectDurationAtServiceLevel() throws AxisFault {
        AxisService service = createService();
        service.addParameter(JMSConstants.PARAM_RECON_MAX_DURATION, "45000");

        ServiceTaskManager stm = createTaskManager(service, new Hashtable<String, String>());

        assertEquals(45000L, stm.getMaxReconnectDuration());
        assertEquals(DEFAULT_THROTTLE_LIMIT_PER_MIN, stm.getThrottleLimitPerMin());
    }

    /**
     * Max reconnect duration set only on the connection factory (listener level) must be applied.
     */
    public void testMaxReconnectDurationAtConnectionFactoryLevel() throws AxisFault {
        Hashtable<String, String> cfParams = new Hashtable<String, String>();
        cfParams.put(JMSConstants.PARAM_RECON_MAX_DURATION, "45000");

        ServiceTaskManager stm = createTaskManager(createService(), cfParams);

        assertEquals(45000L, stm.getMaxReconnectDuration());
    }

    /**
     * The service level value must take precedence over the connection factory value.
     */
    public void testServiceLevelMaxReconnectDurationOverridesConnectionFactory() throws AxisFault {
        AxisService service = createService();
        service.addParameter(JMSConstants.PARAM_RECON_MAX_DURATION, "5000");
        Hashtable<String, String> cfParams = new Hashtable<String, String>();
        cfParams.put(JMSConstants.PARAM_RECON_MAX_DURATION, "45000");

        ServiceTaskManager stm = createTaskManager(service, cfParams);

        assertEquals(5000L, stm.getMaxReconnectDuration());
    }

    /**
     * When both max reconnect duration and throttle limit per minute are configured, each setting
     * must keep its own value.
     */
    public void testMaxReconnectDurationAndThrottleLimitAreIndependent() throws AxisFault {
        AxisService service = createService();
        service.addParameter(JMSConstants.PARAM_RECON_MAX_DURATION, "45000");
        service.addParameter(JMSConstants.JMS_PROXY_THROTTLE_PER_MIN, "2");

        ServiceTaskManager stm = createTaskManager(service, new Hashtable<String, String>());

        assertEquals(45000L, stm.getMaxReconnectDuration());
        assertEquals(2, stm.getThrottleLimitPerMin());
    }

    /**
     * Without the parameter, the default max reconnect duration must be kept.
     */
    public void testDefaultMaxReconnectDurationWhenNotConfigured() throws AxisFault {
        ServiceTaskManager stm = createTaskManager(createService(), new Hashtable<String, String>());

        assertEquals(DEFAULT_MAX_RECONNECT_DURATION, stm.getMaxReconnectDuration());
    }

    /**
     * A non numeric max reconnect duration must be rejected with a descriptive error.
     */
    public void testInvalidMaxReconnectDurationIsRejected() throws AxisFault {
        AxisService service = createService();
        service.addParameter(JMSConstants.PARAM_RECON_MAX_DURATION, "invalid");

        try {
            createTaskManager(service, new Hashtable<String, String>());
            fail("Expected AxisJMSException for a non numeric max reconnect duration");
        } catch (AxisJMSException e) {
            assertTrue(e.getMessage().contains(JMSConstants.PARAM_RECON_MAX_DURATION));
        }
    }

    private AxisService createService() throws AxisFault {
        AxisService service = new AxisService(SERVICE_NAME);
        service.addParameter(JMSConstants.PARAM_CONFAC_JNDI_NAME, "QueueConnectionFactory");
        service.addParameter(JMSConstants.PARAM_DESTINATION, "TestQueue");
        return service;
    }

    private ServiceTaskManager createTaskManager(AxisService service, Hashtable<String, String> cfParams) {
        JMSConnectionFactory jcf = Mockito.mock(JMSConnectionFactory.class);
        Mockito.when(jcf.getParameters()).thenReturn(cfParams);
        return ServiceTaskManagerFactory.createTaskManagerForService(jcf, service, null);
    }
}

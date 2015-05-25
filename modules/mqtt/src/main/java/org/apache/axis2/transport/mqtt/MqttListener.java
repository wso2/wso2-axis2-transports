package org.apache.axis2.transport.mqtt;/*
*  Copyright (c) 2005-2012, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

import org.apache.axis2.AxisFault;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.transport.base.AbstractTransportListenerEx;

public class MqttListener extends AbstractTransportListenerEx<MqttEndpoint> {

    public static final String TRANSPORT_NAME = "mqtt";
    private MqttConnectionFactoryManager connectionFactoryManager;



    @Override
    protected void doInit() throws AxisFault {
        connectionFactoryManager = new MqttConnectionFactoryManager(getTransportInDescription());
        log.info("MQTT Transport Receiver/Listener initialized...");
    }

    @Override
    protected MqttEndpoint createEndpoint() {
        return new MqttEndpoint(this);
    }

    @Override
    protected void startEndpoint(MqttEndpoint mqttEndpoint) throws AxisFault {
       mqttEndpoint.subscribeToTopic(getConfigurationContext());
    }

    @Override
    protected void stopEndpoint(MqttEndpoint mqttEndpoint) {
        mqttEndpoint.unsubscribeFromTopic();
    }

    public MqttConnectionFactory getConnectionFactory(AxisService service) {

        Parameter conFacParam = service.getParameter(MqttConstants.PARAM_MQTT_CONFAC);
        // validate connection factory name (specified or default)
        if (conFacParam != null) {
            return connectionFactoryManager.getMqttConnectionFactory((String) conFacParam.getValue());
        } else {
            return connectionFactoryManager.getMqttConnectionFactory(MqttConstants.DEFAULT_CONFAC_NAME);
        }
    }

}

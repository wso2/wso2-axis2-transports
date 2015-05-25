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

import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

public class MqttConnectionFactoryManager {

      private Log log = LogFactory.getLog(MqttConnectionFactoryManager.class);
      private Map<String,MqttConnectionFactory> connectionFactoryMap = new HashMap<String, MqttConnectionFactory>();


    public MqttConnectionFactoryManager(ParameterInclude trpDesc) {
        loadConnectionFactoryDefinitions(trpDesc);
    }

    private void loadConnectionFactoryDefinitions(ParameterInclude trpDesc) {
        for (Parameter parameter : trpDesc.getParameters()) {
            try {
                MqttConnectionFactory mqttConnectionFactory = new MqttConnectionFactory(parameter);
                connectionFactoryMap.put(mqttConnectionFactory.getName(), mqttConnectionFactory);
            } catch (AxisMqttException e) {
                log.error("Error setting up connection factory : " + parameter.getName(), e);
            }
        }
    }

    public MqttConnectionFactory getMqttConnectionFactory(String name) {
        return connectionFactoryMap.get(name);
    }
}

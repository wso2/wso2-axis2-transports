package org.apache.axis2.transport.mqtt;/*
/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisEngine;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import org.apache.commons.logging.Log;


public class MqttListenerCallback implements MqttCallback {

    private ConfigurationContext configurationContext;
    private MqttEndpoint mqttEndpoint;
    private String contentType;
    private Log log = LogFactory.getLog(MqttListenerCallback.class);


    public MqttListenerCallback(MqttEndpoint mqttEndpoint, String contentType) {
        this.mqttEndpoint = mqttEndpoint;
        this.contentType = contentType;
    }

    public void connectionLost(Throwable throwable) {
        log.error("Connection Lost - Client Disconnected from topic "
                  + mqttEndpoint.getTopic(), throwable);
        mqttEndpoint.subscribeToTopic();
        /**
         * implements from MqttCallback
         */
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        //build the message and hand it over to axisEngine
        MessageContext messageContext = mqttEndpoint.createMessageContext();
        MqttUtils.invoke(mqttMessage, messageContext, contentType);
        AxisEngine.receive(messageContext);
    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        /**
         * implements from MqttCallback
         */
    }

}

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;

public class MqttPublisherCallback implements MqttCallback {
    private String token = null;

    private Log log = LogFactory.getLog(MqttPublisherCallback.class);

    public void connectionLost(Throwable throwable) {
        log.error("Connection Lost");
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        throw new IllegalStateException();
    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        log.info("message delivered .. : " + iMqttDeliveryToken.toString());
    }
}

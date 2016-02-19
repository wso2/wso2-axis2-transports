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

import org.apache.axis2.AxisFault;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.transport.base.AbstractTransportListenerEx;

public class MqttListener extends AbstractTransportListenerEx<MqttEndpoint> {

    public static final String TRANSPORT_NAME = "mqtt";
    private MqttConnectionFactoryManager connectionFactoryManager;
    private String topic;
    private String qos;
    private String contentType;


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

        mqttEndpoint.subscribeToTopic();
    }

    @Override
    protected void stopEndpoint(MqttEndpoint mqttEndpoint) {
        mqttEndpoint.unsubscribeFromTopic();
    }

    public MqttConnectionFactory getConnectionFactory(AxisService service) {
        Parameter conFacParam = service.getParameter(MqttConstants.PARAM_MQTT_CONFAC);
        Parameter topicName = service.getParameter(MqttConstants.MQTT_TOPIC_NAME);
        Parameter qosLevel = service.getParameter(MqttConstants.MQTT_QOS);
        Parameter contentTypeValue = service.getParameter(MqttConstants.CONTENT_TYPE);
        if (topicName != null) {
            setTopic(((String) topicName.getValue()));
        }
        if (qosLevel != null) {
            setQOS(((String) qosLevel.getValue()));
        }
        if (contentTypeValue != null) {
            setContentType(((String) contentTypeValue.getValue()));
        }
        // validate connection factory name (specified or default)
        if (conFacParam != null) {
            return connectionFactoryManager.getMqttConnectionFactory((String) conFacParam.getValue());
        } else {
            return connectionFactoryManager.getMqttConnectionFactory(MqttConstants.DEFAULT_CONFAC_NAME);
        }

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getQOS() {
        return qos;
    }

    public void setQOS(String qos) {
        this.qos = qos;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

}

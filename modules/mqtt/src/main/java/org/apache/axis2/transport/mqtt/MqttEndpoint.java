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
import org.apache.axis2.addressing.EndpointReference;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.transport.base.ProtocolEndpoint;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttTopic;

import java.util.HashSet;
import java.util.Set;

public class MqttEndpoint extends ProtocolEndpoint {

    private Log log = LogFactory.getLog(MqttEndpoint.class);

    private Set<EndpointReference> endpointReferences = new HashSet<EndpointReference>();
    private MqttListener mqttListener;
    private MqttConnectionFactory mqttConnectionFactory;
    private int retryCount = 1000;
    private int retryInterval = 50;
    private MqttClient mqttClient;


    public MqttEndpoint(MqttListener mqttListener) {
        this.mqttListener = mqttListener;
    }

    @Override
    public boolean loadConfiguration(ParameterInclude parameterInclude) throws AxisFault {
        if (!(parameterInclude instanceof AxisService)) {
            return false;
        }

        AxisService service = (AxisService) parameterInclude;

        mqttConnectionFactory = mqttListener.getConnectionFactory(service);
        if (mqttConnectionFactory == null) {
            return false;
        }
        if (mqttListener.getQOS()!=null)
        {
            mqttConnectionFactory.setQos(mqttListener.getQOS());
        }
        return true;
    }

    @Override
    public EndpointReference[] getEndpointReferences(AxisService axisService, String ip) throws AxisFault {
        return new EndpointReference[0];
    }
    public void subscribeToTopic() {
        String clientId = mqttListener.getClientId();
        if(clientId == null) {
            clientId =  mqttConnectionFactory.getClientId();
        }
        mqttClient = mqttConnectionFactory.getMqttClient();

        String contentType = mqttListener.getContentType();
        if (contentType == null){
            contentType=mqttConnectionFactory.getContentType();
        }
        String topic =mqttListener.getTopic();
        if (topic == null){
            topic = mqttConnectionFactory.getTopic();
        }

        int qos;
        if(mqttListener.getQOS() != null) {
            qos = Integer.parseInt(mqttListener.getQOS());
        } else {
            qos = mqttConnectionFactory.getQOS();
        }

        boolean cleanSession ;
        if(mqttListener.getCleanSession() == null) {
            cleanSession = mqttConnectionFactory.getCleanSession();
        } else {
            cleanSession = Boolean.parseBoolean(mqttListener.getCleanSession());
        }
        mqttClient.setCallback(new MqttListenerCallback(this, contentType));
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(cleanSession);
        try {
            mqttClient.connect(options);
            if ( topic!= null) {
                mqttClient.subscribe(topic, qos);
                log.info("Connected to the remote server.");
            }

        }catch (MqttException e){
            if (!mqttClient.isConnected()){
                int retryC = 0;
                while ((retryC < retryCount)) {
                    retryC++;
                    log.info("Attempting to reconnect" + " in " + retryInterval * (retryC + 1) + " ms");
                    try {
                        Thread.sleep(retryInterval * (retryC + 1));
                    } catch (InterruptedException ignore) {
                    }
                    try {
                        mqttClient.connect(options);
                        if(mqttClient.isConnected())
                        {
                            if ( topic!= null) {
                                mqttClient.subscribe(topic, qos);
                            }
                            break;
                        }
                        log.info("Re-connected to the remote server.");
                    } catch (MqttException e1) {
                        log.error("Error while trying to retry", e1);
                    }
                }
            }
        }
    }

    public void unsubscribeFromTopic() {
        if(mqttClient != null) {
            try {
                mqttClient.disconnect();
            } catch (MqttException e) {
                log.error("Error while closing the MQTTClient connection", e);
            }

        }
    }

    protected MqttListener getMqttListener(){
        return mqttListener;
    }

}

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
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.transport.base.ProtocolEndpoint;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

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
    private String topic;
    private int qos;
    private String contentType;
    private boolean cleanSession;
    private String clientId;
    private String hostName;
    private String port;
    private String tempStore;
    private String sslEnabled;


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

        Parameter topicName = service.getParameter(MqttConstants.MQTT_TOPIC_NAME);
        Parameter qosLevel = service.getParameter(MqttConstants.MQTT_QOS);
        Parameter contentTypeValue = service.getParameter(MqttConstants.CONTENT_TYPE);
        Parameter cleanSession = service.getParameter(MqttConstants.MQTT_SESSION_CLEAN);
        Parameter clientId = service.getParameter(MqttConstants.MQTT_CLIENT_ID);
        Parameter hostName = service.getParameter(MqttConstants.MQTT_SERVER_HOST_NAME);
        Parameter port = service.getParameter(MqttConstants.MQTT_SERVER_PORT);
        Parameter sslEnable = service.getParameter(MqttConstants.MQTT_SSL_ENABLE);
        Parameter tempStore = service.getParameter(MqttConstants.MQTT_TEMP_STORE);

        if (topicName != null) {
            setTopic(((String) topicName.getValue()));
        } else {
            setTopic(mqttConnectionFactory.getTopic());
        }
        if (qosLevel != null) {
            setQOS(Integer.parseInt((String) qosLevel.getValue()));
        } else {
            setQOS(mqttConnectionFactory.getQOS());
        }
        if (contentTypeValue != null) {
            setContentType(((String) contentTypeValue.getValue()));
        } else {
            setContentType(mqttConnectionFactory.getContentType());
        }
        if (cleanSession != null) {
            setCleanSession(Boolean.parseBoolean((String) cleanSession.getValue()));
        } else {
            setCleanSession(mqttConnectionFactory.getCleanSession());
        }
        if (clientId != null) {
            setClientId((String) clientId.getValue());
        } else {
            setClientId(mqttConnectionFactory.getClientId());
        }

        if (hostName != null) {
            setHostName((String) hostName.getValue());
        } else {
            setHostName(mqttConnectionFactory.getHostName());
        }

        if (port != null) {
            setPort((String) port.getValue());
        } else {
            setPort(mqttConnectionFactory.getPort());
        }

        if (sslEnable != null) {
            setSSLEnabled((String) sslEnable.getValue());
        } else {
            setSSLEnabled(mqttConnectionFactory.getSSLEnable());
        }

        if (tempStore != null) {
            setTempStore((String) tempStore.getValue());
        } else {
            setTempStore(mqttConnectionFactory.getTempStore());
        }

        return true;
    }

    @Override
    public EndpointReference[] getEndpointReferences(AxisService axisService, String ip) throws AxisFault {
        return new EndpointReference[0];
    }
    public void subscribeToTopic() {
        mqttClient = mqttConnectionFactory.getMqttClient(hostName, port, sslEnabled, clientId, qos
                , tempStore);

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

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setQOS(int qos) {
        this.qos = qos;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setSSLEnabled(String sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public void setTempStore(String tempStore) {
        this.tempStore = tempStore;
    }

    public String getTopic() {
        return this.topic;
    }

}

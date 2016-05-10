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

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.File;
import java.util.Hashtable;

public class MqttConnectionFactory {

    private static final Log log = LogFactory.getLog(MqttConnectionFactory.class);
    private static final String JAVA_IO_TMP_DIR_PROPERTY = "java.io.tmpdir";

    private String name;

    private Hashtable<String, String> parameters = new Hashtable<String, String>();
    private String qosLevel;

    public MqttConnectionFactory(Parameter passedInParameter) {
        this.name = passedInParameter.getName();
        ParameterIncludeImpl parameterInclude = new ParameterIncludeImpl();
        try {
            parameterInclude.deserializeParameters((OMElement) passedInParameter.getParameterElement());
        } catch (AxisFault axisFault) {
            log.error("Error while reading properties for MQTT Connection Factory " + name, axisFault);
            throw new AxisMqttException(axisFault);
        }
        for (Object object : parameterInclude.getParameters()) {
            Parameter parameter = (Parameter) object;
            parameters.put(parameter.getName(), (String) parameter.getValue());
        }
    }

    public MqttConnectionFactory(Hashtable<String, String> parameters) {
        this.parameters = parameters;
    }

    public String getName() {
        return name;
    }

    public MqttClient getMqttClient() {
        return createMqttClient();
    }
    public MqttClient getMqttClient(String clientIdPostFix) {
        //appending the clientIdPostFix to create unique client id
        String uniqueClientId = parameters.get(MqttConstants.MQTT_CLIENT_ID) + "-" + clientIdPostFix;
        return createMqttClient(uniqueClientId);
    }

    public MqttAsyncClient getMqttAsyncClient() {
        String uniqueClientId = parameters.get(MqttConstants.MQTT_CLIENT_ID);
        return createMqttAsyncClient(uniqueClientId);
    }

    public MqttAsyncClient getMqttAsyncClient(String clientIdPostFix) {
        //appending the clientIdPostFix to create unique client id
        String uniqueClientId = parameters.get(MqttConstants.MQTT_CLIENT_ID) + "-" + clientIdPostFix;
        return createMqttAsyncClient(uniqueClientId);
    }

    private MqttClient createMqttClient() {
        String uniqueClientId = parameters.get(MqttConstants.MQTT_CLIENT_ID);
        return createMqttClient(uniqueClientId);
    }

    private MqttClient createMqttClient(String uniqueClientId) {
        String sslEnable = parameters.get(MqttConstants.MQTT_SSL_ENABLE);
        MqttDefaultFilePersistence dataStore = getDataStore(uniqueClientId);

        String mqttEndpointURL = "tcp://" + parameters.get(MqttConstants.MQTT_SERVER_HOST_NAME) + ":" +
                                 parameters.get(MqttConstants.MQTT_SERVER_PORT);
        // If SSL is enabled in the config, Use SSL tranport
        if (sslEnable != null && sslEnable.equalsIgnoreCase("true")) {
            mqttEndpointURL = "ssl://" + parameters.get(MqttConstants.MQTT_SERVER_HOST_NAME) + ":" +
                              parameters.get(MqttConstants.MQTT_SERVER_PORT);
        }
        MqttClient mqttClient = null;
        if(log.isDebugEnabled()){
            log.debug("ClientId " + uniqueClientId);
        }
        try {
            mqttClient = new MqttClient(mqttEndpointURL, uniqueClientId, dataStore);

        } catch (MqttException e) {
            log.error("Error while creating the MQTT client...", e);
            throw new AxisMqttException("Error while creating the MQTT client", e);
        }
        return mqttClient;
    }



    private MqttAsyncClient createMqttAsyncClient(String uniqueClientId) {
        String sslEnable = parameters.get(MqttConstants.MQTT_SSL_ENABLE);
        MqttDefaultFilePersistence dataStore = getDataStore(uniqueClientId);

        String mqttEndpointURL = "tcp://" + parameters.get(MqttConstants.MQTT_SERVER_HOST_NAME) + ":" +
                parameters.get(MqttConstants.MQTT_SERVER_PORT);
        // If SSL is enabled in the config, Use SSL tranport
        if (sslEnable != null && sslEnable.equalsIgnoreCase("true")) {
            mqttEndpointURL = "ssl://" + parameters.get(MqttConstants.MQTT_SERVER_HOST_NAME) + ":" +
                    parameters.get(MqttConstants.MQTT_SERVER_PORT);
        }

        MqttAsyncClient mqttClient = null;
        try {
            mqttClient = new MqttAsyncClient(mqttEndpointURL, uniqueClientId, dataStore);
        } catch (MqttException e) {
            log.error("Error while creating the MQTT client...", e);
        }
        return mqttClient;
    }
    public String getTopic() {
        return parameters.get(MqttConstants.MQTT_TOPIC_NAME);
    }
    public String getContentType() {
        return parameters.get(MqttConstants.CONTENT_TYPE);
    }
    public void setQos(String qosLevel) {
        this.qosLevel = qosLevel;
    }

    /**
     * This sample stores in a temporary directory... where messages temporarily
     * stored until the message has been delivered to the server.
     * a real application ought to store them somewhere
     * where they are not likely to get deleted or tampered with
     *
     * @return the MqttDefaultFilePersistence
     */
    private MqttDefaultFilePersistence getDataStore(String uniqueClientId) {
        MqttDefaultFilePersistence dataStore = null;
        String tmpDir = parameters.get(MqttConstants.MQTT_TEMP_STORE);
        String qosValue = qosLevel;
        if (qosValue == null) {
            qosValue = parameters.get(MqttConstants.MQTT_QOS);
        }
        if(uniqueClientId != null && !uniqueClientId.isEmpty()){
            uniqueClientId = File.separator + uniqueClientId;
        } else {
            uniqueClientId = "";
        }
        if (qosValue != null) {
            int qos = Integer.parseInt(qosValue);
            {
                if (qos == 2 || qos == 1) {
                    if (tmpDir != null) {
                        dataStore = new MqttDefaultFilePersistence(tmpDir + uniqueClientId);
                    } else {
                        tmpDir = System.getProperty(JAVA_IO_TMP_DIR_PROPERTY);
                        dataStore = new MqttDefaultFilePersistence(tmpDir + uniqueClientId);
                    }
                }
                if (qos == 0) {
                    dataStore = null;
                }
            }
        } else {
            if (tmpDir != null) {
                dataStore = new MqttDefaultFilePersistence(tmpDir + uniqueClientId);
            } else {
                tmpDir = System.getProperty(JAVA_IO_TMP_DIR_PROPERTY);
                dataStore = new MqttDefaultFilePersistence(tmpDir + uniqueClientId);
            }
        }

        return dataStore;
    }

    public boolean getCleanSession() {
        String cleanSession = parameters.get(MqttConstants.MQTT_SESSION_CLEAN);

        if(cleanSession != null && cleanSession.isEmpty()) {
            return Boolean.parseBoolean(cleanSession);
        } else {
            //default value
            return true;
        }
    }

    public int getQOS() {
        String qos = qosLevel;
        if (qos == null) {
            qos = parameters.get(MqttConstants.MQTT_QOS);
        }
        if (qos != null && !qos.isEmpty()) {
            return Integer.parseInt(qos);
        } else {
            //default value
            return 0;
        }
    }

    public String getClientId() {
        return parameters.get(MqttConstants.MQTT_CLIENT_ID);
    }
}

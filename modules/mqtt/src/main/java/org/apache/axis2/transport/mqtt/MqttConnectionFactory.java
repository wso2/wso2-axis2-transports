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

    /**
     * Creating a MqttClient
     * @param uniqueClientId clientId for Data Store
     * @param qos qos value
     * @return MqttClient
     */
    public MqttClient getMqttClient(String uniqueClientId, int qos) {
        return createMqttClient(uniqueClientId, qos);
    }

    /**
     * Creating a MqttClient
     * @param hostName hostname of MQTT Server
     * @param port post of MQTT Server
     * @param sslEnable whether SSL is enable or not, values true of false
     * @param uniqueClientId clientId for Data Store
     * @param qos qos value
     * @param tempStore location for data store
     * @return MqttClient
     */
    public MqttClient getMqttClient(String hostName, String port, String sslEnable
            , String uniqueClientId, int qos, String tempStore) {
        return createMqttClient(hostName, port, sslEnable, uniqueClientId, qos, tempStore);
    }

    /**
     * Creating the MqttAsyncClient
     * @param uniqueClientId - clientId for Data Store
     * @param qos qos value
     * @return MqttAsyncClient
     */
    public MqttAsyncClient getMqttAsyncClient(String uniqueClientId, int qos) {
        return createMqttAsyncClient(uniqueClientId, qos);
    }

    private MqttClient createMqttClient(String uniqueClientId, int qos) {
        String sslEnable = parameters.get(MqttConstants.MQTT_SSL_ENABLE);
        String tempStore = parameters.get(MqttConstants.MQTT_TEMP_STORE);
        String hostName = parameters.get(MqttConstants.MQTT_SERVER_HOST_NAME);
        String port = parameters.get(MqttConstants.MQTT_SERVER_PORT);
        return createMqttClient(hostName, port, sslEnable, uniqueClientId, qos, tempStore);
    }

    private MqttClient createMqttClient(String hostName, String port, String sslEnable
            , String uniqueClientId, int qos, String tempStore) {
        MqttDefaultFilePersistence dataStore = getDataStore(uniqueClientId, qos, tempStore);

        String mqttEndpointURL = hostName + ":" + port;
        // If SSL is enabled in the config, Use SSL tranport
        if (sslEnable != null && sslEnable.equalsIgnoreCase("true")) {
            mqttEndpointURL = "ssl://" + mqttEndpointURL;
        } else {
            mqttEndpointURL = "tcp://" + mqttEndpointURL;
        }
        MqttClient mqttClient;
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

    private MqttAsyncClient createMqttAsyncClient(String uniqueClientId, int qos) {
        String sslEnable = parameters.get(MqttConstants.MQTT_SSL_ENABLE);
        String tempStore = parameters.get(MqttConstants.MQTT_TEMP_STORE);
        MqttDefaultFilePersistence dataStore = getDataStore(uniqueClientId, qos, tempStore);

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

    /**
     * This sample stores in a temporary directory... where messages temporarily
     * stored until the message has been delivered to the server.
     * a real application ought to store them somewhere
     * where they are not likely to get deleted or tampered with
     *
     * @return the MqttDefaultFilePersistence
     */
    private MqttDefaultFilePersistence getDataStore(String uniqueClientId, int qos, String tmpStore) {
        MqttDefaultFilePersistence dataStore = null;
        String tmpDir;
        if (tmpStore == null || tmpStore.isEmpty()) {
            tmpDir = parameters.get(MqttConstants.MQTT_TEMP_STORE);
        } else {
            tmpDir = tmpStore;
        }

        if(uniqueClientId != null && !uniqueClientId.isEmpty()){
            uniqueClientId = File.separator + uniqueClientId;
        } else {
            uniqueClientId = "";
        }
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
        String qos = parameters.get(MqttConstants.MQTT_QOS);
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

    public String getHostName() {
        return parameters.get(MqttConstants.MQTT_SERVER_HOST_NAME);
    }

    public String getPort() {
        return parameters.get(MqttConstants.MQTT_SERVER_PORT);
    }

    public String getSSLEnable() {
        return parameters.get(MqttConstants.MQTT_SSL_ENABLE);
    }

    public String getTempStore() {
        return parameters.get(MqttConstants.MQTT_TEMP_STORE);
    }

    public String getTopic() {
        return parameters.get(MqttConstants.MQTT_TOPIC_NAME);
    }

    public String getName() {
        return name;
    }
    public String getContentType() {
        return parameters.get(MqttConstants.CONTENT_TYPE);
    }
}

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

import org.apache.axiom.om.OMOutputFormat;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.base.AbstractTransportSender;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.commons.io.output.WriterOutputStream;
import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Hashtable;
import java.util.Random;

public class MqttSender extends AbstractTransportSender {

    private MqttConnectionFactoryManager connectionFactoryManager;


    @Override
    public void init(ConfigurationContext cfgCtx, TransportOutDescription transportOutDescription) throws AxisFault {
        super.init(cfgCtx, transportOutDescription);
        connectionFactoryManager = new MqttConnectionFactoryManager(transportOutDescription);
        log.info("Mqtt Transport Sender initialized...");
    }

    @Override
    public void sendMessage(MessageContext messageContext, String targetEPR, OutTransportInfo outTransportInfo) throws AxisFault{
        Hashtable<String, String> properties;
        properties = BaseUtils.getEPRProperties(targetEPR);
        MqttConnectOptions mqttConnectOptions;
        String mqttBlockingSenderEnable;
        MqttConnectionFactory mqttConnectionFactory;
        String username = properties.get(MqttConstants.MQTT_USERNAME);
        String password = properties.get(MqttConstants.MQTT_PASSWORD);
        String cleanSession = properties.get(MqttConstants.MQTT_SESSION_CLEAN);
        String topicName = properties.get(MqttConstants.MQTT_TOPIC_NAME);
        String clientId = properties.get(MqttConstants.MQTT_CLIENT_ID);
        String qosValue = properties.get(MqttConstants.MQTT_QOS);
        String retainedMessage = properties.get(MqttConstants.MQTT_MESSAGE_RETAINED);
        mqttBlockingSenderEnable = properties.get(MqttConstants.MQTT_BLOCKING_SENDER);
        int qos;
        /* Default value is set to false */
        boolean isMessageRetained = false;

        mqttConnectOptions = new MqttConnectOptions();
        if (cleanSession != null) {
            mqttConnectOptions.setCleanSession(Boolean.parseBoolean(cleanSession));
        }
        if (password != null) {
            mqttConnectOptions.setPassword(password.toCharArray());
        }
        if (username != null) {
            mqttConnectOptions.setUserName(username);
        }

        mqttConnectionFactory = new MqttConnectionFactory(properties);

        if (qosValue != null && !qosValue.isEmpty()) {
            qos =  Integer.parseInt(qosValue);
        } else {
            qos = mqttConnectionFactory.getQOS();
        }
        if (retainedMessage != null && !retainedMessage.isEmpty()) {
            isMessageRetained =  Boolean.parseBoolean(retainedMessage);
        }
        MqttClient mqttClient;
        MqttAsyncClient mqttAsyncClient;
        if (mqttBlockingSenderEnable != null && mqttBlockingSenderEnable.equalsIgnoreCase("true")) {
            //removing the urnid:urnid:
            String clientIdPostFix = messageContext.getMessageID().substring(9);
            String uniqueClientId;
            //appending the clientIdPostFix to create unique client id
            if(clientId == null) {
                uniqueClientId = clientIdPostFix;
            } else {
                uniqueClientId = clientId + "-" + clientIdPostFix;
            }

            //sending the message id to make client id unique. Otherwise accessing same store with multiple
            //thread make errors
            mqttClient = mqttConnectionFactory.getMqttClient(uniqueClientId, qos);
            try {
                mqttClient.setCallback(new MqttPublisherCallback());
                mqttClient.connect(mqttConnectOptions);

                if (mqttClient.isConnected()) {
                    if (topicName == null) {
                        handleException("The request doesn't contain the required topic fields");
                    }
                    MqttTopic mqttTopic = mqttClient.getTopic(topicName);
                    MqttMessage mqttMessage = createMqttMessage(messageContext);
                    mqttMessage.setRetained(isMessageRetained);

                    if (qos >= 0 && qos <= 2) {
                        mqttMessage.setQos(qos);
                    } else {
                        throw new AxisFault("Invalid value for qos " + qos);
                    }
                    mqttTopic.publish(mqttMessage);
                }
            } catch (MqttException e) {
                handleException("Exception occurred at sending message", e);
            }finally {
                if (mqttClient!=null) {
                    try {
                        mqttClient.disconnect();
                    } catch (MqttException e) {
                        log.error("Error while disconnecting the mqtt client", e);
                    }
                }
            }
        } else {
            //removing the urnid:urnid:
            String clientIdPostFix = messageContext.getMessageID().substring(9);
            //sending the message id to make client id unique. Otherwise accessing same store with multiple
            //thread make errors
            //appending the clientIdPostFix to create unique client id
            String uniqueClientId = clientId + "-" + clientIdPostFix;
            mqttAsyncClient = mqttConnectionFactory.getMqttAsyncClient(uniqueClientId, qos);
            try {
                MqttAsyncCallback mqttAsyncClientCallback = new MqttAsyncCallback(mqttAsyncClient);
                mqttAsyncClientCallback.setConOpt(mqttConnectOptions);
                MqttMessage mqttMessage = createMqttMessage(messageContext);
                mqttMessage.setRetained(isMessageRetained);

                if ((qos >= 0 && qos <= 2)) {
                    mqttMessage.setQos(qos);
                } else {
                    throw new AxisFault("Invalid value for qos " + qos);
                }

                mqttAsyncClientCallback.publish(topicName, mqttMessage);
            } catch (MqttException me) {
                handleException("Exception occurred at sending message", me);
            } catch (Throwable th) {
                if (th instanceof Exception) {
                    handleException("Exception occurred while sending message", (Exception) th);
                } else {
                    log.error("Exception occurred while sending message", th);
                }
            }
        }
    }

    private MqttMessage createMqttMessage(MessageContext messageContext) {
        OMOutputFormat format = BaseUtils.getOMOutputFormat(messageContext);
        MessageFormatter messageFormatter;
        try {
            messageFormatter = MessageProcessorSelector.getMessageFormatter(messageContext);
        } catch (AxisFault axisFault) {
            throw new AxisMqttException("Unable to get the message formatter to use");
        }
        OutputStream out;
        StringWriter sw = new StringWriter();
        try {
            out = new WriterOutputStream(sw, format.getCharSetEncoding());
        } catch (UnsupportedCharsetException ex) {
            throw new AxisMqttException("Unsupported encoding " + format.getCharSetEncoding(), ex);
        }
        try {
            messageFormatter.writeTo(messageContext, format, out, true);
            out.close();
        } catch (IOException e) {
            throw new AxisMqttException("IO Error while creating BytesMessage", e);
        }
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(sw.toString().getBytes());
        return mqttMessage;
    }
}

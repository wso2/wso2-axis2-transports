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

public class MqttSender extends AbstractTransportSender {

    private MqttConnectionFactoryManager connectionFactoryManager;
    private Hashtable<String, String> properties = new Hashtable<String, String>();
    private MqttConnectOptions mqttConnectOptions;
    private String mqttBlockingSenderEnable;

    MqttConnectionFactory mqttConnectionFactory;

    @Override
    public void init(ConfigurationContext cfgCtx, TransportOutDescription transportOutDescription) throws AxisFault {
        super.init(cfgCtx, transportOutDescription);
        connectionFactoryManager = new MqttConnectionFactoryManager(transportOutDescription);
        log.info("Mqtt transport sender initialized....");
    }

    @Override
    public void sendMessage(MessageContext messageContext, String targetEPR, OutTransportInfo outTransportInfo) throws AxisFault{
        properties = BaseUtils.getEPRProperties(targetEPR);
        String username = properties.get(MqttConstants.MQTT_USERNAME);
        String password = properties.get(MqttConstants.MQTT_PASSWORD);
        String cleanSession = properties.get(MqttConstants.MQTT_SESSION_CLEAN);
        String topicName = properties.get(MqttConstants.MQTT_TOPIC_NAME);

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
        mqttBlockingSenderEnable = properties.get(MqttConstants.MQTT_BLOCKING_SENDER);
        properties = BaseUtils.getEPRProperties(targetEPR);
        String qos = properties.get(MqttConstants.MQTT_QOS);
        MqttClient mqttClient;
        MqttAsyncClient mqttAsyncClient;
        if (mqttBlockingSenderEnable != null && mqttBlockingSenderEnable.equalsIgnoreCase("true")) {
            mqttClient = mqttConnectionFactory.getMqttClient();
            try {
                mqttClient.setCallback(new MqttPublisherCallback());
                mqttClient.connect(mqttConnectOptions);

                if (mqttClient.isConnected()) {
                    if (topicName == null) {
                        log.error("The request doesn't contain the required topic fields");
                    }
                    MqttTopic mqttTopic = mqttClient.getTopic(topicName);
                    MqttMessage mqttMessage = createMqttMessage(messageContext);
                    mqttMessage.setRetained(true);
                    if (qos != null) {
                        int qosValue = Integer.parseInt(qos);
                        try {
                            if (qosValue >=0 && qosValue <=2) {
                                mqttMessage.setQos(qosValue);
                            } else {
                                throw new AxisFault("Invalid value for qos");
                            }
                        } catch (AxisMqttException e){handleException("Invalid value for qos: ", e);}
                    }
                    mqttTopic.publish(mqttMessage);
                }
            } catch (MqttException e) {
                throw new AxisFault("Exception occured at sending message");
            }finally {
                if (mqttClient!=null) {
                    try {
                        mqttClient.disconnect();
                    } catch (MqttException e) {
                        log.error("Error while disconnecting the mqtt client");
                    }
                }
            }
        } else {

            mqttAsyncClient = mqttConnectionFactory.getMqttAsyncClient();
            try {
                MqttAsyncCallback mqttAsyncClientCallback = new MqttAsyncCallback(mqttAsyncClient);
                mqttAsyncClientCallback.setConOpt(mqttConnectOptions);
                MqttMessage mqttMessage = createMqttMessage(messageContext);
                mqttMessage.setRetained(true);
                if (qos != null) {
                    int qosValue = Integer.parseInt(qos);
                    if (qosValue < 0 || qosValue > 2) {
                        log.info("Invalid value for qos. It should be an integer between 0 and 2");
                    } else {
                        mqttMessage.setQos(qosValue);
                    }
                }
                mqttAsyncClientCallback.publish(topicName, mqttMessage);
            } catch (MqttException me) {
                log.error("Exception occured at sending message", me);
            } catch (Throwable th) {
                log.error("Exception occured while sending message",th);
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

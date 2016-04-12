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

public class MqttConstants {
    public static final String PARAM_MQTT_CONFAC = "mqtt.connection.factory";
    public static final String DEFAULT_CONFAC_NAME = "default";
    public static final String MQTT_SERVER_HOST_NAME = "mqtt.server.host.name";
    public static final String MQTT_SERVER_PORT = "mqtt.server.port";
    public static final String MQTT_CLIENT_ID = "mqtt.client.id";
    public static final String MQTT_TOPIC_NAME = "mqtt.topic.name";
    public static final String MQTT_QOS = "mqtt.subscription.qos";
    public static final String MQTT_SESSION_CLEAN = "mqtt.session.clean";
    public static final String MQTT_SSL_ENABLE = "mqtt.ssl.enable";
    public static final String MQTT_USERNAME = "mqtt.subscription.username";
    public static final String MQTT_PASSWORD = "mqtt.subscription.password";
    public static final String MQTT_TEMP_STORE = "mqtt.temporary.store.directory";
    public static final String MQTT_BLOCKING_SENDER = "mqtt.blocking.sender";
    public static final String CONTENT_TYPE = "mqtt.content.type";
    public static final String TEMP_DIR = "java.io.tmpdir";
    public static final int WAIT_TIME = 10000;
    public static final int QOS_VALUE_ONE = 1;
    public static final int QOS_VALUE_TWO = 2;
    public static final int QOS_VALUE_ZERO = 0;

}

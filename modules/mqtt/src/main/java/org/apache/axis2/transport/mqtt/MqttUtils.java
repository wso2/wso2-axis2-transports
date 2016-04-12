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
package org.apache.axis2.transport.mqtt;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.apache.axis2.format.TextMessageBuilderAdapter;
import org.apache.axis2.format.TextMessageBuilder;
import javax.jms.TextMessage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class MqttUtils {

    private static Log log = LogFactory.getLog(MqttUtils.class);

    /**
     * Builds the message according to the content type
     * @throws MqttException
     */
    public static void invoke(MqttMessage mqttMessage, MessageContext msgContext, String contentType)
            throws AxisFault, AxisMqttException {
        if (StringUtils.isEmpty(contentType)) {
            contentType = "text/plain";
        }
        Builder builder = BuilderUtil.getBuilderFromSelector(contentType, msgContext);
        if (builder == null) {
            if (log.isDebugEnabled()) {
                log.debug("No message builder found for type '" + contentType + "'. Falling back to SOAP.");
            }
            builder = new SOAPBuilder();
        }
        msgContext.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "UTF-8");
        OMElement documentElement = null;
        byte[] bytes = mqttMessage.getPayload();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        documentElement = builder.processDocument(byteArrayInputStream, contentType, msgContext);
        msgContext.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
        try {
            byteArrayInputStream.close();
        } catch (IOException e) {
            log.error("Error while closing the InputStream",e);
        }
    }
}

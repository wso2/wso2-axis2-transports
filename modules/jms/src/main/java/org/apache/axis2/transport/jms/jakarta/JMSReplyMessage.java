/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.jms.jakarta;

import org.apache.axis2.context.MessageContext;

import jakarta.jms.Destination;
import java.util.Map;

/**
 * This class is used to hold the JMS response message information.
 */
public class JMSReplyMessage {

    private MessageContext msgctx;
    private Map<String, Object> transportHeaders;
    private String soapAction;
    private String contentType;
    Destination replyDestination;

    public Destination getReplyDestination() {

        return replyDestination;
    }

    public void setReplyDestination(Destination replyDestination) {

        this.replyDestination = replyDestination;
    }

    public MessageContext getMsgctx() {

        return msgctx;
    }

    public void setMsgctx(MessageContext msgctx) {

        this.msgctx = msgctx;
    }

    public Map<String, Object> getTransportHeaders() {

        return transportHeaders;
    }

    public void setTransportHeaders(Map<String, Object> transportHeaders) {

        this.transportHeaders = transportHeaders;
    }

    public String getSoapAction() {

        return soapAction;
    }

    public void setSoapAction(String soapAction) {

        this.soapAction = soapAction;
    }

    public String getContentType() {

        return contentType;
    }

    public void setContentType(String contentType) {

        this.contentType = contentType;
    }

}

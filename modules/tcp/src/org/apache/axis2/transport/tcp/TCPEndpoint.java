/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.axis2.transport.tcp;

import org.apache.axis2.transport.base.ProtocolEndpoint;
import org.apache.axis2.transport.base.ParamUtils;
import org.apache.axis2.addressing.EndpointReference;
import org.apache.axis2.AxisFault;
import org.apache.axis2.util.Utils;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.description.AxisService;

import java.net.SocketException;

public class TCPEndpoint extends ProtocolEndpoint {

    private String host = null;
    private int port = -1;
    private int backlog = TCPConstants.TCP_DEFAULT_BACKLOG;
    private String contentType;
    private String recordDelimiter;
    private String recordDelimiterType;
    private String responseDelimiter;
    private String responseDelimiterType;
    private Integer recordLength;
    private boolean clientResponseRequired;
    private String inputType;

    public TCPEndpoint() {

    }

    public TCPEndpoint(String host, int port, int backlog) {
        this.host = host;
        this.port = port;
        this.backlog = backlog;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public int getBacklog() {
        return backlog;
    }

    public String getContentType() {
        return contentType;
    }

    public String getRecordDelimiter() {
        return recordDelimiter;
    }

    public Integer getRecordLength() {
        return recordLength;
    }

    public boolean isClientResponseRequired() {
		return clientResponseRequired;
	}

    public void setClientResponseRequired(boolean clientResponseRequired) {
        this.clientResponseRequired = clientResponseRequired;
    }

    public String getInputType() {
    	return inputType;
    }

    public void setInputType(String inputType) {
    	this.inputType = inputType;
    }

    public String getRecordDelimiterType() {
        return recordDelimiterType;
    }

    public void setRecordDelimiterType(String recordDelimiterType) {
        this.recordDelimiterType = recordDelimiterType;
    }

    public boolean loadConfiguration(ParameterInclude params) throws AxisFault {
        port = ParamUtils.getOptionalParamInt(params, TCPConstants.PARAM_PORT, -1);
        if (port == -1) {
            return false;
        }

        contentType = ParamUtils.getOptionalParam(params, TCPConstants.PARAM_CONTENT_TYPE);
        if (contentType == null || contentType.isEmpty()) {
            contentType = TCPConstants.TCP_DEFAULT_CONTENT_TYPE;
        }
        
        recordDelimiter = ParamUtils.getOptionalParam(params, TCPConstants.PARAM_RECORD_DELIMITER);
        if (recordDelimiter == null) {
        	recordDelimiter = "";
        }

        responseDelimiter = ParamUtils.getOptionalParam(params, TCPConstants.PARAM_RESPONSE_DELIMITER);
        responseDelimiterType = ParamUtils.getOptionalParam(params, TCPConstants.PARAM_RESPONSE_DELIMITER_TYPE);
        if (responseDelimiterType == null || responseDelimiterType.isEmpty()) {
            responseDelimiterType = TCPConstants.CHARACTER_DELIMITER_TYPE;
        }

        recordLength =  ParamUtils.getOptionalParamInt(params, TCPConstants.PARAM_RECORD_LENGTH);
        if(recordLength == null){
        	recordLength = -1;
        }
        
        inputType  =  ParamUtils.getOptionalParam(params, TCPConstants.PARAM_RESPONSE_INPUT_TYPE);
        if(inputType == null || inputType.isEmpty()){
        	inputType = TCPConstants.BINARY_INPUT_TYPE;
        }

        recordDelimiterType  =  ParamUtils.getOptionalParam(params, TCPConstants.PARAM_RECORD_DELIMITER_TYPE);
        if(recordDelimiterType == null || recordDelimiterType.isEmpty() ){
            recordDelimiterType = TCPConstants.CHARACTER_DELIMITER_TYPE;
        }

        clientResponseRequired =  ParamUtils.getOptionalParamBoolean(params, TCPConstants.PARAM_RESPONSE_CLIENT, false);

        host = ParamUtils.getOptionalParam(params, TCPConstants.PARAM_HOST);
        backlog = ParamUtils.getOptionalParamInt(params, TCPConstants.PARAM_BACKLOG, TCPConstants.TCP_DEFAULT_BACKLOG);
        return true;
    }

    public EndpointReference[] getEndpointReferences(AxisService service,
                                                     String ip) throws AxisFault {
        if (host == null && ip == null) {
            try {
                ip = Utils.getIpAddress(getListener().getConfigurationContext().
                        getAxisConfiguration());
            } catch (SocketException ex) {
                throw new AxisFault("Unable to determine the host's IP address", ex);
            }
        }

        String url = "tcp://" + (host != null ? host : ip) + ":" + port;
        String context = getListener().getConfigurationContext().getServiceContextPath();
        url +=  (context.startsWith("/") ? "" : "/") + context +
                (context.endsWith("/") ? "" : "/") +
                (getService() == null ? service.getName() : getServiceName());

        if (!contentType.equals(TCPConstants.TCP_DEFAULT_CONTENT_TYPE)) {
            url += "?contentType=" + contentType;
        }
        return new EndpointReference[] { new EndpointReference(url) };
    }

    public String getResponseDelimiterType() {
        return responseDelimiterType;
    }

    public String getResponseDelimiter() {
        return responseDelimiter;
    }
}

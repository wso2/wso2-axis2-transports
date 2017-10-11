/*
 *  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.axis2.transport.tcp;

import org.apache.axis2.transport.OutTransportInfo;

import java.net.Socket;

public class TCPOutTransportInfo implements OutTransportInfo {

	private Socket socket;
	private String contentType;
	private boolean clientResponseRequired = false;
	private String delimiter;
	private String delimiterType;
	private int recordDelimiterLength;

	public Socket getSocket() {
		return socket;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getContentType() {
		return contentType;
	}

	public boolean isClientResponseRequired() {
		return clientResponseRequired;
	}

	public void setClientResponseRequired(boolean clientResponseRequired) {
		this.clientResponseRequired = clientResponseRequired;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getDelimiterType() {
		return delimiterType;
	}

	public void setDelimiterType(String delimiterType) {
		this.delimiterType = delimiterType;
	}

	public int getRecordDelimiterLength() {
		return recordDelimiterLength;
	}

	public void setRecordDelimiterLength(int recordDelimiterLength) {
		this.recordDelimiterLength = recordDelimiterLength;
	}
}

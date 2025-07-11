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

public class TCPConstants {

    public static final String PARAM_PORT = "transport.tcp.port";
    public static final String PARAM_HOST = "transport.tcp.hostname";
    public static final String PARAM_BACKLOG = "transport.tcp.backlog";
    public static final String PARAM_CONTENT_TYPE = "transport.tcp.contentType";
    public static final String PARAM_RECORD_DELIMITER = "transport.tcp.recordDelimiter";
    public static final String PARAM_RECORD_DELIMITER_TYPE = "transport.tcp.recordDelimiterType";
    public static final String PARAM_RESPONSE_DELIMITER = "transport.tcp.responseDelimiter";
    public static final String PARAM_RESPONSE_DELIMITER_TYPE = "transport.tcp.responseDelimiterType";
    public static final String PARAM_RECORD_LENGTH = "transport.tcp.recordLength";
    public static final String PARAM_RESPONSE_CLIENT = "transport.tcp.responseClient";
    public static final String PARAM_RESPONSE_INPUT_TYPE = "transport.tcp.inputType";
    public static final String BINARY_INPUT_TYPE = "binary";
    public static final String STRING_INPUT_TYPE = "string";
    public static final String STRING_DELIMITER_TYPE = "string";
    public static final String BYTE_DELIMITER_TYPE = "byte";
    public static final String CHARACTER_DELIMITER_TYPE = "character";
    public static final int TCP_DEFAULT_BACKLOG = 50;
    public static final String TCP_DEFAULT_CONTENT_TYPE = "text/xml";
    public static final String TCP_OUTPUT_SOCKET = "transport.tcp.outputSocket";
}

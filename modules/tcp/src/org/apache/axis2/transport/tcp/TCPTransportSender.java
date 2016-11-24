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

import org.apache.axiom.attachments.ByteArrayDataSource;
import org.apache.axiom.om.*;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.OutInAxisOperation;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisEngine;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.base.AbstractTransportSender;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axiom.om.OMOutputFormat;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.xml.namespace.QName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.Map;
import java.util.HashMap;

public class TCPTransportSender extends AbstractTransportSender {

    Map<Integer, Socket> persitentConnections = new HashMap<>();

    public void sendMessage(MessageContext msgContext, String targetEPR,
                            OutTransportInfo outTransportInfo) throws AxisFault {

        if (targetEPR != null) {
            Integer clientId = (Integer)msgContext.getProperty("clientId");
            Socket socket = persitentConnections.get(clientId);
            Map<String, String> params = getURLParameters(targetEPR);

            if (msgContext.getProperty("websocket.source.handshake.present") != null && (boolean)msgContext.getProperty("websocket.source.handshake.present")) {
                int timeout = -1;
                if (params.containsKey("timeout")) {
                    timeout = Integer.parseInt(params.get("timeout"));
                }

                if (socket == null) {
                    persitentConnections.put(clientId, openTCPConnection(targetEPR, timeout));
                }
                return;
            }
            if (msgContext.getProperty("websocket.terminate") != null && (boolean)msgContext.getProperty("websocket.terminate")) {
                persitentConnections.remove(clientId);
                return;
            }
            //Socket socket = openTCPConnection(targetEPR, timeout);

            //msgContext.setProperty(TCPConstants.TCP_OUTPUT_SOCKET, socket);

            try {
                writeMessageOut(msgContext, socket.getOutputStream(), params.get("delimiter"),
                        params.get("delimiterType"), params.get("delimiterLength"));
                if (!msgContext.getOptions().isUseSeparateListener() && !msgContext.isServerSide()) {
                    waitForReply(msgContext, socket, params.get("contentType"), params.get("delimiterType"),
                            params.get("delimiterLength"));
                }
            } catch (IOException e) {
                handleException("Error while sending a TCP request", e);
            } finally {
                //closeConnection(socket);
            }

        } else if (outTransportInfo != null && (outTransportInfo instanceof TCPOutTransportInfo)) {
            TCPOutTransportInfo outInfo = (TCPOutTransportInfo) outTransportInfo;
            try {
                writeMessageOut(msgContext, outInfo.getSocket().getOutputStream(), outInfo.getDelimiter(),
                        outInfo.getDelimiterType(), null);
            } catch (IOException e) {
                handleException("Error while sending a TCP response", e);
            } finally {
                if (!outInfo.isClientResponseRequired()) {
                    closeConnection(outInfo.getSocket());
                }

            }
        }
    }

    /**
     * Writing the message to the output stream of the TCP socket after applying correct message formatter
     * This method is synchronized because there will be issue when formatter write to same output stream which causes
     * to mixed messages
     *
     * @param msgContext   the message context
     * @param outputStream the socket output stream
     * @throws AxisFault   if error occurred
     * @throws IOException if IO exception occurred
     */
    private synchronized void writeMessageOut(MessageContext msgContext,
                                              OutputStream outputStream, String delimiter, String delimiterType,
                                              String delimiterLength) throws AxisFault, IOException {
        MessageFormatter messageFormatter = BaseUtils.getMessageFormatter(msgContext);
        OMOutputFormat format = BaseUtils.getOMOutputFormat(msgContext);
        if (delimiterLength == null) {
            messageFormatter.writeTo(msgContext, format, outputStream, true);
            if (delimiter != null && !delimiter.isEmpty()) {
                if (TCPConstants.BYTE_DELIMITER_TYPE.equalsIgnoreCase(delimiterType)) {
                    outputStream.write((char) Integer.parseInt(delimiter.split("0x")[1], 16));
                } else {
                    outputStream.write(delimiter.getBytes());
                }
            }
        } else {
            byte[] message = messageFormatter.getBytes(msgContext, format);
            byte[] delimiterByteArray = getDelimiter(msgContext, message, delimiterType, Integer.parseInt(delimiterLength));
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream( );
            byteArrayOutputStream.write(delimiterByteArray);
            byteArrayOutputStream.write(message);
            outputStream.write(byteArrayOutputStream.toByteArray());
        }
        outputStream.flush();
    }

    @Override
    public void cleanup(MessageContext msgContext) throws AxisFault {
        Object socketObj = msgContext.getProperty(TCPConstants.TCP_OUTPUT_SOCKET);
        if (socketObj != null) {
            closeConnection((Socket) socketObj);
        }
    }

    private void waitForReply(MessageContext msgContext, Socket socket,
                              String contentType, String delimiterType, String delimiterLength) throws AxisFault {

        if (!(msgContext.getAxisOperation() instanceof OutInAxisOperation) &&
                msgContext.getProperty(org.apache.axis2.Constants.PIGGYBACK_MESSAGE) == null) {
            return;
        }

        if (contentType == null) {
            contentType = TCPConstants.TCP_DEFAULT_CONTENT_TYPE;
        }

        try {
            MessageContext responseMsgCtx = createResponseMessageContext(msgContext);
            //SOAPEnvelope envelope = TransportUtils.createSOAPMessage(msgContext,
            //socket.getInputStream(), contentType);
            OMElement documentElement = createDocumentElement(new ByteArrayDataSource(getMessage(msgContext, socket,
                    delimiterType, Integer.parseInt(delimiterLength))), msgContext);
            SOAPEnvelope envelope = TransportUtils.createSOAPEnvelope(documentElement);
            responseMsgCtx.setEnvelope(envelope);
            responseMsgCtx.setProperty("backendMessageType", "binary/octet-stream");
            AxisEngine.receive(responseMsgCtx);
        } catch (Exception e) {
            handleException("Error while processing response", e);
        }
    }

    private Map<String, String> getURLParameters(String url) throws AxisFault {
        try {
            Map<String, String> params = new HashMap<String, String>();
            URI tcpUrl = new URI(url);
            String query = tcpUrl.getQuery();
            if (query != null) {
                String[] paramStrings = query.split("&");
                for (String p : paramStrings) {
                    int index = p.indexOf('=');
                    params.put(p.substring(0, index), p.substring(index + 1));
                }
            }
            return params;
        } catch (URISyntaxException e) {
            handleException("Malformed tcp url", e);
        }
        return null;
    }

    private Socket openTCPConnection(String url, int timeout) throws AxisFault {
        try {
            URI tcpUrl = new URI(url);
            if (!tcpUrl.getScheme().equals("tcp")) {
                throw new Exception("Invalid protocol prefix : " + tcpUrl.getScheme());
            }
            SocketAddress address = new InetSocketAddress(tcpUrl.getHost(), tcpUrl.getPort());
            Socket socket = new Socket();
            if (timeout != -1) {
                socket.setSoTimeout(timeout);
            }
            socket.connect(address);
            return socket;
        } catch (Exception e) {
            handleException("Error while opening TCP connection to : " + url, e);
        }
        return null;
    }

    private void closeConnection(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            log.error("Error while closing a TCP socket", e);
        }
    }

    public static OMElement createDocumentElement(DataSource dataSource, MessageContext msgContext) {
        QName wrapperQName = BaseConstants.DEFAULT_BINARY_WRAPPER;
        if (msgContext.getAxisService() != null) {
            Parameter factory = msgContext.getAxisService().getParameter("Wrapper");
            if (factory != null) {
                wrapperQName = BaseUtils.getQNameFromString(factory.getValue());
            }
        }

        OMFactory factory = OMAbstractFactory.getOMFactory();
        OMElement wrapper = factory.createOMElement(wrapperQName, (OMContainer) null);
        wrapper.addChild(factory.createOMText(new DataHandler(dataSource), true));

        msgContext.setDoingMTOM(true);

        return wrapper;
    }

    public static final byte[] getMessage(MessageContext msgContext, Socket socket, String delimiterType,
                                          int delimiterLength) throws AxisFault {
        byte[] message = null;
        try {
            int messageLength = 0;
            int messageLengthCount = 0;
            int delimiterLengthCount = 0;
            int bufferCount = 0;
            int remaining;
            byte[] buffer = new byte[4096];
            InputStream inputStream = socket.getInputStream();
            ByteArrayOutputStream delimiterStream = new ByteArrayOutputStream();
            ByteArrayOutputStream messageStream = new ByteArrayOutputStream();

            while (delimiterLengthCount < delimiterLength) {
                bufferCount = inputStream.read(buffer);
                if ((delimiterLengthCount + bufferCount) >= delimiterLength) {
                    remaining = delimiterLength - delimiterLengthCount;
                    delimiterStream.write(buffer, 0, remaining);

                    messageLengthCount = bufferCount - remaining;
                    if (messageLengthCount > 0) {
                        messageStream.write(buffer, remaining, messageLengthCount);
                    }

                    break;
                } else {
                    delimiterStream.write(buffer, 0, bufferCount);
                    delimiterLengthCount += bufferCount;
                }
            }

            if (delimiterType.equalsIgnoreCase("binary")) {
                messageLength = convertBinaryBytesToInt(delimiterStream.toByteArray());
            } else {
                messageLength = convertAsciiBytesToInt(delimiterStream.toByteArray());
            }

            while (messageLengthCount < messageLength) {
                bufferCount = inputStream.read(buffer);
                if ((messageLengthCount + bufferCount) >= messageLength) {
                    remaining = messageLength - messageLengthCount;
                    messageStream.write(buffer, 0, remaining);
                    break;
                } else {
                    messageStream.write(buffer, 0, bufferCount);
                    messageLengthCount += bufferCount;
                }
            }

            message = messageStream.toByteArray();
        } catch (IOException e) {
            throw new AxisFault("Unable to read message payload", e);
        }

        return message;
    }

    public static final byte[] getDelimiter(MessageContext msgContext, byte[] message, String delimiterType,
                                            int delimiterLength) {
        byte[] delimiter;

        if (delimiterType.equalsIgnoreCase("binary")) {
            delimiter = convertIntToBinaryBytes(message.length, delimiterLength);
        } else {
            delimiter = convertIntToAsciiBytes(message.length, delimiterLength);
        }

        return delimiter;
    }

    public static final byte[] convertIntToBinaryBytes(int input, int byteSize) {
        if (byteSize > 4) {
            byteSize = 4;
        }

        byte[] output = new byte[byteSize];
        int[] factor = {0xFF000000, 0x00FF0000, 0x0000FF00, 0x000000FF};
        int slot = 4 - byteSize;
        int mult = byteSize;

        for (int i = 0; i < byteSize; i++) {
            output[i] = (byte) ((input & factor[slot + i]) >> (8 * (--mult)));
        }

        return output;
    }

    public static int convertBinaryBytesToInt(byte[] input) {
        int output = 0;
        byte[] bytes;

        if (input.length > 4) {
            bytes = new byte[4];
            System.arraycopy(input, (input.length - 4), bytes, 0, 4);
        } else {
            bytes = input;
        }

        for (int i = 0; i < bytes.length; i++) {
            output = ((output << 8) & 0xFFFFFF00) + (0x000000FF & bytes[i]);
        }

        return output;
    }

    public static final byte[] convertIntToAsciiBytes(int input, int byteSize) {
        if (byteSize > 4) {
            byteSize = 4;
        }

        byte[] output = new byte[byteSize];
        int[] factor = {1000, 100, 10, 1};
        int slot = 4 - byteSize;
        int mod = input;

        for (int i = 0; i < byteSize; i++) {
            output[i] = (byte) ((mod / factor[slot + i] & 0x0f) | 0x30);
            mod = mod % factor[slot + i];
        }

        return output;
    }

    public static int convertAsciiBytesToInt(byte[] input) {
        int output = 0;
        int[] factor = {1000, 100, 10, 1};
        int slot = 4 - input.length;

        for (int i = 0; i < input.length; i++) {
            output += (input[i] & 0x0f) * factor[slot + i];
        }

        return output;
    }
}

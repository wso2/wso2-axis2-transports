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
import javax.xml.stream.XMLStreamException;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPTransportSender extends AbstractTransportSender {

    private static final Map<String, PersistentConnectionInfoBean> persistentConnectionInfoBeanMap = new HashMap<>();
    private static final Map<String, ThreadHolderBean> threadHolder = new HashMap<>();
    private static final Map<String, BlockingQueue> exceptionMapper = new HashMap<>();
    private static final Map<String, BlockingQueue<MessageContextToResponseContext>> messageContextHolder = new HashMap<>();

    private static boolean exceptionMapperStarted = false;

    public void sendMessage(MessageContext msgContext, String targetEPR, OutTransportInfo outTransportInfo)
            throws AxisFault {
        if (targetEPR != null) {
            Map<String, String> params = getURLParameters(targetEPR);
            String isPersistent = params.get(TCPConstants.IS_PERSISTENT);
            String retryInterval = params.get(TCPConstants.RETRY_INTERVAL);
            int noOfRetries = params.get(TCPConstants.NO_OF_RETRIES) != null ? Integer.parseInt(
                    params.get(TCPConstants.NO_OF_RETRIES)) : -1;
            int timeout = -1;
            if (params != null && params.containsKey(TCPConstants.TIMEOUT)) {
                timeout = Integer.parseInt(params.get(TCPConstants.TIMEOUT));
            }

            String clientId = null;

            if (msgContext.getProperty(TCPConstants.CLIENT_ID) != null) {
                clientId = msgContext.getProperty(TCPConstants.CLIENT_ID).toString();
            }
            Socket socket;

            if (isPersistent != null && Boolean.parseBoolean(isPersistent)) {

                if ( msgContext.getProperty(TCPConstants.SOURCE_HANDSHAKE_PRESENT) == null ||
                        msgContext.getProperty(TCPConstants.SOURCE_HANDSHAKE_PRESENT) != null
                        && !msgContext.getProperty(TCPConstants.SOURCE_HANDSHAKE_PRESENT).equals(true)) {

                    BlockingQueue messageContexts = messageContextHolder.get(clientId);

                    if (messageContexts == null) {
                        messageContexts = new LinkedBlockingQueue();
                        messageContextHolder.put(clientId, messageContexts);
                    }

                    MessageContext responseMsgCtx = createResponseMessageContext(msgContext);
                    responseMsgCtx.setProperty(TCPConstants.BACKEND_MESSAGE_TYPE, TCPConstants.BINARY_OCTET_STREAM);
                    MessageContextToResponseContext messageContextToResponseContext =
                            new MessageContextToResponseContext();
                    messageContextToResponseContext.setMessageContext(msgContext);
                    messageContextToResponseContext.setResponseContext(responseMsgCtx);
                    messageContexts.add(messageContextToResponseContext);
                }
            }

            if (isPersistent != null && Boolean.parseBoolean(isPersistent)) {
                PersistentConnectionInfoBean persistentConnectionInfoBean = persistentConnectionInfoBeanMap.get(clientId);
                if (persistentConnectionInfoBean == null) {
                    persistentConnectionInfoBean = new PersistentConnectionInfoBean();
                    persistentConnectionInfoBean.setMessagesQueue(new LinkedBlockingQueue<byte[]>());
                    persistentConnectionInfoBeanMap.put(clientId, persistentConnectionInfoBean);
                }
                socket = persistentConnectionInfoBean.getPersistentConnection();

                if (socket == null && clientId != null) {
                    socket = openTCPConnection(targetEPR, timeout, retryInterval);
                    if (socket == null) {
                        throw new AxisFault("Could not create connection to server.");
                    }
                    persistentConnectionInfoBean.setPersistentConnection(socket);

                    BlockingQueue<Exception> exceptions = exceptionMapper.get(clientId);

                    if (exceptions == null) {
                        exceptions = new LinkedBlockingQueue<>();
                        exceptionMapper.put(clientId, exceptions);
                    }

                    BlockingQueue messageContexts = messageContextHolder.get(clientId);

                    if (messageContexts == null) {
                        messageContexts = new LinkedBlockingQueue();
                        messageContextHolder.put(clientId, messageContexts);
                    }


                    ThreadHolderBean threadHolderBean = threadHolder.get(clientId);
                    if (threadHolderBean == null) {

                        PersistentConnectionInfoBean persistentConnectionInfoBean1 = persistentConnectionInfoBeanMap.get(clientId);

                        BlockingQueue<byte[]> messages = persistentConnectionInfoBean1.getMessagesQueue();

                        Thread listenerThread = new Thread(new TCPBackendListener(messages, persistentConnectionInfoBean1.getPersistentConnection(), exceptions));
                        Thread senderThread = new Thread(new TCPResponseSender(
                                messageContexts, messages, Integer.parseInt(params.get(TCPConstants.DELIMITER_LENGTH)),
                                params.get(TCPConstants.DELIMITER_TYPE), params.get(TCPConstants.CONTENT_TYPE)));

                        threadHolderBean = new ThreadHolderBean();
                        threadHolderBean.setBackendListener(listenerThread);
                        threadHolderBean.setResponseSender(senderThread);

                        if (!listenerThread.isAlive()) {
                            listenerThread.start();
                        }

                        if (!senderThread.isAlive()) {
                            senderThread.start();
                        }

                        threadHolder.put(clientId, threadHolderBean);
                    }

                    if (!exceptionMapperStarted) {
                        exceptionMapperStarted = true;
                        BlockingQueue<Exception> exceptionsQueue = exceptionMapper.get(clientId);
                        try {
                            if (exceptionsQueue.take() != null) {
                                socket = null;
                                cleanPersistentDataHolders(clientId);
                                if (retryInterval != null) {
                                    try {
                                        int retries = 0;
                                        while (socket == null) {
                                            Thread.sleep(Long.parseLong(retryInterval));
                                            socket = openTCPConnection(targetEPR, timeout, retryInterval);
                                            if (socket != null) {
                                                persistentConnectionInfoBeanMap.get(clientId).setPersistentConnection(socket);
                                                return;
                                            } else {
                                                retries++;
                                                if (noOfRetries != -1 && noOfRetries == retries) {
                                                    exceptionMapperStarted = false;
                                                    throw new AxisFault("Failed to connect to backend server.");
                                                }
                                            }
                                        }
                                    } catch (InterruptedException e1) {
                                        log.error("Error occurred while waiting for next connection retry.", e1);
                                    }

                                }
                                exceptionMapperStarted = false;
                                throw new AxisFault("Error occurred while reading or writing to backend.");
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                if (msgContext.getProperty(TCPConstants.SOURCE_HANDSHAKE_PRESENT) != null
                        && msgContext.getProperty(TCPConstants.SOURCE_HANDSHAKE_PRESENT).equals(true)) {
                    return;
                }
            } else {
                socket = openTCPConnection(targetEPR, timeout, retryInterval);
            }
            if (isPersistent != null && Boolean.parseBoolean(isPersistent)) {
                Object terminateProperty = msgContext.getProperty(TCPConstants.CONNECTION_TERMINATE);
                if (terminateProperty != null && (boolean) terminateProperty) {
                    exceptionMapperStarted = false;
                    cleanPersistentDataHolders(clientId);
                    return;
                }
            }

            try {

                writeMessageOut(msgContext, socket.getOutputStream(), params.get(TCPConstants.DELIMITER),
                        params.get(TCPConstants.DELIMITER_TYPE), params.get(TCPConstants.DELIMITER_LENGTH));
                if (!msgContext.getOptions().isUseSeparateListener() && !msgContext.isServerSide()) {
                    waitForReply(msgContext, socket.getInputStream(), params);
                }

            } catch (IOException e) {
                socket = null;
                if (isPersistent != null && Boolean.parseBoolean(isPersistent)) {
                    cleanPersistentDataHolders(clientId);
                }
                if (retryInterval != null) {
                    try {
                        int retries = 0;
                        while (socket == null) {
                            Thread.sleep(Long.parseLong(retryInterval));
                            socket = openTCPConnection(targetEPR, timeout, retryInterval);
                            if (socket != null) {
                                persistentConnectionInfoBeanMap.get(clientId).setPersistentConnection(socket);
                                return;
                            } else {
                                retries++;
                                if (noOfRetries != -1 && noOfRetries == retries) {
                                    throw new AxisFault("Failed to connect to backend server.");
                                }
                            }
                        }
                    } catch (InterruptedException e1) {
                        log.error("Error occurred while waiting for next connection retry.", e1);
                    }

                }
                handleException("Error while sending a TCP request", e);
            }
        } else if (outTransportInfo != null && (outTransportInfo instanceof TCPOutTransportInfo)) {
            TCPOutTransportInfo outInfo = (TCPOutTransportInfo) outTransportInfo;
            try {
                writeMessageOut(msgContext, outInfo.getSocket().getOutputStream(), outInfo.getDelimiter(),
                        outInfo.getDelimiterType(), Integer.toString(outInfo.getRecordDelimiterLength()));
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
    private synchronized void writeMessageOut(MessageContext msgContext, OutputStream outputStream, String delimiter,
                                              String delimiterType, String delimiterLength) throws IOException {
        MessageFormatter messageFormatter = BaseUtils.getMessageFormatter(msgContext);
        OMOutputFormat format = BaseUtils.getOMOutputFormat(msgContext);
        if (delimiterLength.equalsIgnoreCase("-1")) {
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
            byte[] delimiterByteArray = getDelimiter(msgContext, message, delimiterType,
                    Integer.parseInt(delimiterLength));
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
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

    private void waitForReply(MessageContext msgContext, InputStream inputStream, Map<String, String> params) throws AxisFault {

        String contentType = params.get(TCPConstants.CONTENT_TYPE);
        String delimiterLength = params.get(TCPConstants.DELIMITER_LENGTH);
        String isPersistent = params.get(TCPConstants.IS_PERSISTENT);
        String delimiterType = params.get(TCPConstants.DELIMITER_TYPE);


        if (!(msgContext.getAxisOperation() instanceof OutInAxisOperation) &&
                msgContext.getProperty(org.apache.axis2.Constants.PIGGYBACK_MESSAGE) == null) {
            return;
        }

        if (contentType == null) {
            contentType = TCPConstants.TCP_DEFAULT_CONTENT_TYPE;
        }

        try {
            MessageContext responseMsgCtx = createResponseMessageContext(msgContext);
            SOAPEnvelope envelope;
            if (delimiterLength == null) {
                envelope = TransportUtils.createSOAPMessage(msgContext, inputStream, contentType);
                responseMsgCtx.setEnvelope(envelope);
                AxisEngine.receive(responseMsgCtx);
            } else {
                if (isPersistent == null || (isPersistent != null && !Boolean.parseBoolean(isPersistent))) {
                    responseMsgCtx.setProperty(TCPConstants.BACKEND_MESSAGE_TYPE, TCPConstants.BINARY_OCTET_STREAM);
                    getMessage(msgContext, inputStream,delimiterType, Integer.parseInt(delimiterLength),
                            contentType, responseMsgCtx);
                }
            }
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

    private Socket openTCPConnection(String url, int timeout, String retryInterval) throws AxisFault {
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
            if (retryInterval == null) {
                handleException("Error while opening TCP connection to : " + url, e);
            }
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
            Parameter factory = msgContext.getAxisService().getParameter(TCPConstants.WRAPPER);
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

    private void getMessage(MessageContext msgContext,InputStream inputStream,
                                          String delimiterType, int delimiterLength, String contentType, MessageContext responseMsgCtx)
            throws AxisFault, XMLStreamException {
        try {
            int messageLength;

            ExcessAndReadBytes excessAndReadBytesFromReadDelimitingLength;
            ExcessAndReadBytes excessAndReadBytesFromReadMessageLength = new ExcessAndReadBytes();
            excessAndReadBytesFromReadMessageLength.setExcessBytes(new byte[0]);

            do {
                excessAndReadBytesFromReadDelimitingLength = readUntilLength(
                        excessAndReadBytesFromReadMessageLength.getExcessBytes(),delimiterLength, inputStream);

                messageLength = getMessageLength(delimiterType, excessAndReadBytesFromReadDelimitingLength.getReadBytes());

                excessAndReadBytesFromReadMessageLength = readUntilLength(excessAndReadBytesFromReadDelimitingLength.getExcessBytes(),
                        messageLength, inputStream);

                responseMsgCtx.setEnvelope(TransportUtils.createSOAPMessage(msgContext,
                        new ByteArrayInputStream(excessAndReadBytesFromReadMessageLength.getReadBytes()), contentType));
                AxisEngine.receive(responseMsgCtx);

            } while (excessAndReadBytesFromReadMessageLength.getExcessBytes()!= null &&
                    excessAndReadBytesFromReadMessageLength.getExcessBytes().length > 0);

        } catch (IOException e) {
            throw new AxisFault("Unable to read message payload", e);
        }
    }

    private byte[] getDelimiter(MessageContext msgContext, byte[] message, String delimiterType, int
            delimiterLength) {
        byte[] delimiter = null;
        if (delimiterType.equalsIgnoreCase(TCPConstants.BINARY_DELIMITER_TYPE)) {
            delimiter = TCPUtils.convertIntToBinaryBytes(message.length, delimiterLength);
        } else if (delimiterType.equalsIgnoreCase(TCPConstants.ASCII_DELIMITER_TYPE)) {
            delimiter = TCPUtils.convertIntToAsciiBytes(message.length, delimiterLength);
        }
        return delimiter;
    }

    /**
     *
     * @param excessReadBytes Excess read bytes from previous read.
     * @param length Length to be read.
     * @param inputStream Input stream to be read.
     * @return ExcessAndReadBytes object which contains read bytes and excess read bytes.
     */
    private  ExcessAndReadBytes readUntilLength(byte[] excessReadBytes, int length, InputStream inputStream) throws AxisFault {
        ExcessAndReadBytes excessAndReadBytes = new ExcessAndReadBytes();
        byte[] buffer = new byte[4096];
        int readLengthCount = 0, bufferCount = 0, remaining, excessLengthCount;
        ByteArrayOutputStream readStream = new ByteArrayOutputStream();
        boolean excessBytesAssigned = false;
        try {
            while (readLengthCount < length) {
                if(excessReadBytes.length > 0 && !excessBytesAssigned) {
                    bufferCount = excessReadBytes.length;
                    buffer = excessReadBytes;
                    excessBytesAssigned = true;
                } else {
                    bufferCount = inputStream.read(buffer);
                }
                if ((readLengthCount + bufferCount) >= length) {
                    remaining = length - readLengthCount;
                    readStream.write(buffer, 0, remaining);
                    excessLengthCount = bufferCount - remaining;
                    if (excessLengthCount > 0) {
                        excessAndReadBytes.setExcessBytes(Arrays.copyOfRange(buffer, remaining, bufferCount));
                    }
                    break;
                } else {
                    readStream.write(buffer, 0, bufferCount);
                    readLengthCount += bufferCount;
                }
            }
        } catch (IOException e) {
           handleException("Error occurred while reading data from backend", e);
        }
        excessAndReadBytes.setReadBytes(readStream.toByteArray());
        return excessAndReadBytes;
    }

    /**
     *
     * @param delimiterType Delimiter type binary or ascii.
     * @param delimiterBytes Delimiter length encoded in binary or ascii.
     * @return Decoded message length.
     */
    private int getMessageLength(String delimiterType, byte[] delimiterBytes) {
        if (delimiterType.equalsIgnoreCase(TCPConstants.BINARY_DELIMITER_TYPE)) {
           return TCPUtils.convertBinaryBytesToInt(delimiterBytes);
        } else if (delimiterType.equalsIgnoreCase(TCPConstants.ASCII_DELIMITER_TYPE)) {
           return  TCPUtils.convertAsciiBytesToInt(delimiterBytes);
        }
        return -1;
    }

    private void cleanPersistentDataHolders(String clientId) {
        persistentConnectionInfoBeanMap.remove(clientId);
        exceptionMapper.remove(clientId);
        threadHolder.remove(clientId);
        messageContextHolder.remove(clientId);

    }
}

package org.apache.axis2.transport.tcp;


import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisEngine;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TCPResponseSender implements Runnable {

    private static final Log log = LogFactory.getLog(TCPResponseSender.class);

    private BlockingQueue<MessageContextToResponseContext> msgCtxToResponseMsgCtxs;
    private BlockingQueue<byte[]> messages;
    private int delimiterLength;
    private String delimiterType;
    private String contentType;

    public TCPResponseSender(BlockingQueue<MessageContextToResponseContext> msgCtxToResponseMsgCtxs,
                             BlockingQueue<byte[]> messages, int delimiterLength, String delimiterType, String contentType) {
        this.msgCtxToResponseMsgCtxs = msgCtxToResponseMsgCtxs;
        this.messages = messages;
        this.delimiterLength = delimiterLength;
        this.delimiterType = delimiterType;
        this.contentType = contentType;
    }

    @Override
    public void run() {
        log.info("sender thread started.................!!!");
        MessageContextToResponseContext msgCtxToResponseMsgCtx;
        byte[] message;
        ByteArrayOutputStream readStream = new ByteArrayOutputStream();
        try {
            // message = messages.take();
            while ((message = messages.take()) != null) {
                readStream.write(message);
                while (readStream.size() > 0 && (msgCtxToResponseMsgCtx = msgCtxToResponseMsgCtxs.take()) != null) {
                    while (readStream.size() < delimiterLength) {
                        readStream.write(messages.take());
                    }

                    byte[] readBuffer = readStream.toByteArray();

                    byte[] delimiterBuffer = Arrays.copyOfRange(readBuffer, 0 , delimiterLength);

                    readStream.reset();

                    readStream.write(Arrays.copyOfRange(readBuffer, delimiterLength , readBuffer.length));

                    int messageLength = getMessageLength(delimiterType, delimiterBuffer);

                    while (readStream.size() < messageLength) {
                        readStream.write(messages.take());
                    }

                    readBuffer = readStream.toByteArray();

                    byte[] messageToSendBack = Arrays.copyOfRange(readBuffer, 0, messageLength);

                    MessageContext msgContext = msgCtxToResponseMsgCtx.getMessageContext();
                    MessageContext responseMsgCtx = msgCtxToResponseMsgCtx.getResponseContext();

                    responseMsgCtx.setEnvelope(TransportUtils.createSOAPMessage(msgContext,
                        new ByteArrayInputStream(messageToSendBack), contentType));
                    AxisEngine.receive(responseMsgCtx);

                    readStream.reset();

                    readBuffer = trim(readBuffer);

                    readStream.write(Arrays.copyOfRange(readBuffer, messageLength, readBuffer.length));
                }
            }

        } catch (InterruptedException e) {
            log.error("Error occurred while polling message contexts queue. ", e);
        } catch (IOException e) {
            log.error("Error while reading or writing data. ", e);
        } catch (XMLStreamException e) {
            log.error("Error occurred while creating soap message. ", e);
        }

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


    private byte[] trim(byte[] bytes)
    {
        int i = bytes.length - 1;
        while (i >= 0 && bytes[i] == 0)
        {
            --i;
        }

        return Arrays.copyOf(bytes, i + 1);
    }
}

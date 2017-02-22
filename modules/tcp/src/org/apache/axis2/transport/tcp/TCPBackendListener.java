package org.apache.axis2.transport.tcp;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPBackendListener implements Runnable {

    private static final Log log = LogFactory.getLog(TCPBackendListener.class);

    public TCPBackendListener(BlockingQueue<byte[]> messagesQueue, Socket socket, BlockingQueue<Exception> exceptions) {
        this.messagesQueue = messagesQueue;
        this.socket = socket;
        this.exceptionsQueue = exceptions;
    }

    Socket socket;
    byte[] buffer;
    BlockingQueue<byte[]> messagesQueue;
    BlockingQueue<Exception> exceptionsQueue;

    @Override
    public void run() {
        while (true) {

            int bufferCount;
            try {
                buffer = new byte[4096];
                bufferCount = socket.getInputStream().read(buffer);
                if (bufferCount == -1) {
                    throw new Exception("TCP connection broken.");
                } else {
                    messagesQueue.add(buffer);
                }
            } catch (Exception e) {
                log.error("Error occurred while reading from input stream.", e);
                exceptionsQueue.add(e);
                break;
            }
        }
    }
}

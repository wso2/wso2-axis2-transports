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
    byte[] buffer = new byte[4096];
    BlockingQueue<byte[]> messagesQueue;
    BlockingQueue<Exception> exceptionsQueue;

    @Override
    public void run() {
        log.info("listener thread started...........!!!!!");
        while (true) {

            int bufferCount;
            try {
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

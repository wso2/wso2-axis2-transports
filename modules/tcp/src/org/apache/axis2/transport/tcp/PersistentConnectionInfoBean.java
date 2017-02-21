package org.apache.axis2.transport.tcp;

import org.apache.axis2.context.MessageContext;

import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;


public class PersistentConnectionInfoBean {

    private Socket persistentConnection;
    private Queue<MessageContext> responseMessageContexts;
    private Thread listenerThread;
    private BlockingQueue<byte[]> messagesQueue;

    public Socket getPersistentConnection() {
        return persistentConnection;
    }

    public void setPersistentConnection(Socket persistentConnection) {
        this.persistentConnection = persistentConnection;
    }

    public Queue<MessageContext> getResponseMessageContexts() {
        return responseMessageContexts;
    }

    public void setResponseMessageContexts(Queue<MessageContext> responseMessageContexts) {
        this.responseMessageContexts = responseMessageContexts;
    }

    public Thread getListenerThread() {
        return listenerThread;
    }

    public void setListenerThread(Thread listenerThread) {
        this.listenerThread = listenerThread;
    }

    public BlockingQueue<byte[]> getMessagesQueue() {
        return messagesQueue;
    }

    public void setMessagesQueue(BlockingQueue<byte[]> messagesQueue) {
        this.messagesQueue = messagesQueue;
    }

}

package org.apache.axis2.transport.tcp;


public class ThreadHolderBean {

    Thread backendListener;
    Thread responseSender;

    public Thread getBackendListener() {
        return backendListener;
    }

    public void setBackendListener(Thread backendListener) {
        this.backendListener = backendListener;
    }

    public Thread getResponseSender() {
        return responseSender;
    }

    public void setResponseSender(Thread responseSender) {
        this.responseSender = responseSender;
    }
}

package org.apache.axis2.transport.tcp;


import org.apache.axis2.context.MessageContext;

public class MessageContextToResponseContext {

    MessageContext messageContext;
    MessageContext responseContext;

    public MessageContext getMessageContext() {
        return messageContext;
    }

    public void setMessageContext(MessageContext messageContext) {
        this.messageContext = messageContext;
    }

    public MessageContext getResponseContext() {
        return responseContext;
    }

    public void setResponseContext(MessageContext responseContext) {
        this.responseContext = responseContext;
    }
}

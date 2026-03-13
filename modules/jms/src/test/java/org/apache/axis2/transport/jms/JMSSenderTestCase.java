/*
* Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.axis2.transport.jms;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.engine.AxisConfiguration;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import javax.jms.Destination;
import javax.jms.Session;
import javax.transaction.*;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;

public class JMSSenderTestCase extends TestCase {

    private String assertErrorMessageForTrue = "waitForResponse is false and destination is null";
    private String assertErrorMessageForFalse = "either waitForResponse is true or destination is not null";

    public void testIsWaitForResponseOrReplyDestinationBothTrue() throws Exception {
        JMSSender jmsSender = new JMSSender();
        Destination destination = Mockito.mock(Destination.class);
        boolean result = jmsSender.isWaitForResponseOrReplyDestination(true, destination);
        Assert.assertTrue(assertErrorMessageForTrue, result);
    }

    public void testIsWaitForResponseOrReplyDestinationWhenOneIsTrue() throws Exception {
        JMSSender jmsSender = new JMSSender();
        boolean result1 = jmsSender.isWaitForResponseOrReplyDestination(true, null);
        Assert.assertTrue(assertErrorMessageForTrue, result1);

        Destination destination = Mockito.mock(Destination.class);
        boolean result2 = jmsSender.isWaitForResponseOrReplyDestination(false, destination);
        Assert.assertTrue(assertErrorMessageForTrue, result2);
    }

    public void testIsWaitForResponseOrReplyDestinationWhenBothFalse() throws Exception {
        JMSSender jmsSender = new JMSSender();
        boolean result = jmsSender.isWaitForResponseOrReplyDestination(false, null);
        Assert.assertFalse(assertErrorMessageForFalse, result);
    }

    /**
     * Test case for EI-1244.
     * test transport.jms.TransactionCommand parameter in transport url when sending the message.
     * This will verify the fixes which prevent possible OOM issue when publishing messages to a broker using jms.
     *
     * @throws Exception
     */
    public void testTransactionCommandParameter() throws Exception {
        JMSSender jmsSender = Mockito.spy(new JMSSender());
        JMSMessageSender jmsMessageSender = Mockito.mock(JMSMessageSender.class);
        Session session = Mockito.mock(Session.class);

        Mockito.doReturn(session).when(jmsMessageSender).getSession();

        try (MockedConstruction<JMSOutTransportInfo> mockedJmsOut = Mockito.mockConstruction(JMSOutTransportInfo.class,
                (mock, context) -> {
                    Mockito.doReturn(jmsMessageSender).when(mock).createJMSSender(any(MessageContext.class));
                })) {

            Mockito.doReturn(null).when(jmsSender).getJMSConnectionFactory(any(JMSOutTransportInfo.class));
            Mockito.doReturn(new JMSReplyMessage())
                    .when(jmsSender).sendOverJMS(any(), any(), any(), any(), any());

            jmsSender.init(new ConfigurationContext(new AxisConfiguration()), new TransportOutDescription("jms"));
            MessageContext messageContext = new MessageContext();
            //append the transport.jms.TransactionCommand
            String targetAddress = "jms:/SimpleStockQuoteService?transport.jms.ConnectionFactoryJNDIName="
                    + "QueueConnectionFactory&transport.jms.TransactionCommand=begin"
                    + "&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory";
            Transaction transaction = new TestJMSTransaction();
            messageContext.setProperty(JMSConstants.JMS_XA_TRANSACTION, transaction);

            jmsSender.sendMessage(messageContext, targetAddress, null);

            Field field = JMSSender.class.getDeclaredField("jmsMessageSenderMap");
            field.setAccessible(true);
            Map<Transaction, ArrayList<JMSMessageSender>> jmsMessageSenderMap =
                    (Map<Transaction, ArrayList<JMSMessageSender>>) field.get(null);
            Assert.assertEquals("Transaction not added to map", 1, jmsMessageSenderMap.size());
            List senderList = jmsMessageSenderMap.get(transaction);
            Assert.assertNotNull("List is null", senderList);
            Assert.assertEquals("List is empty", 1, senderList.size());
        }
    }

    public void testGetContentTypePropertyNameFromFactory() {
        final String contentTypeProperty = "contentType";
        JMSSender jmsSender = new JMSSender();
        MessageContext ctx = new MessageContext();

        //setting up factory with content type property
        JMSConnectionFactory factory = Mockito.mock(JMSConnectionFactory.class);
        Hashtable<String, String> paramTable = new Hashtable<>();
        paramTable.put(JMSConstants.CONTENT_TYPE_PROPERTY_PARAM, contentTypeProperty);
        Mockito.when(factory.getParameters()).thenReturn(paramTable);

        JMSOutTransportInfo jmsOutTransportInfo = new JMSOutTransportInfo(factory, null, null);
        String contentTypePropertyResult = jmsSender.getContentTypePropertyForJavax(ctx, jmsOutTransportInfo, factory);
        Assert.assertEquals(contentTypeProperty, contentTypePropertyResult);
    }

    /**
     * Test case for EI-2776.
     * Test whether the transport.jms.MessagePropertyHyphens parameter is set to the message context.
     * @throws Exception
     */
    public void testGettingHyphenModeFromJMSConnectionFactory() throws Exception {
        JMSSender jmsSender = Mockito.spy(new JMSSender());
        Session session = Mockito.mock(Session.class);

        final String hyphenMode = "replace";
        JMSConnectionFactory jmsConnectionFactory = Mockito.mock(JMSConnectionFactory.class);
        Hashtable<String, String> paramTable = new Hashtable<>();
        paramTable.put(JMSConstants.PARAM_JMS_HYPHEN_MODE, hyphenMode);
        Mockito.when(jmsConnectionFactory.getParameters()).thenReturn(paramTable);

        try (MockedConstruction<JMSOutTransportInfo> mockedJmsOut = Mockito.mockConstruction(JMSOutTransportInfo.class);
             MockedConstruction<JMSMessageSender> mockedSender = Mockito.mockConstruction(JMSMessageSender.class,
                     (mock, context) -> {
                         Mockito.doReturn(session).when(mock).getSession();
                     })) {

            Mockito.doReturn(jmsConnectionFactory).when(jmsSender).getJMSConnectionFactory(any(JMSOutTransportInfo.class));
            Mockito.doReturn(new JMSReplyMessage())
                    .when(jmsSender).sendOverJMS(any(), any(), any(), any(), any());

            jmsSender.init(new ConfigurationContext(new AxisConfiguration()), new TransportOutDescription("jms"));
            MessageContext messageContext = new MessageContext();
            jmsSender.sendMessage(messageContext, "jms:/SimpleStockQuoteService", null);

            Assert.assertEquals("Hyphen mode provided by the JMSConnectionFactory has not been set " +
                                "to the message context", hyphenMode,
                                messageContext.getProperty(JMSConstants.PARAM_JMS_HYPHEN_MODE));
        }
    }

    /**
     * Test class which implement javax.Transaction for test transport.jms.TransactionCommand.
     */
    private class TestJMSTransaction implements Transaction {

        @Override
        public void commit()
                throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException,
                SystemException {

        }

        @Override
        public boolean delistResource(XAResource xaResource, int i) throws IllegalStateException, SystemException {
            return false;
        }

        @Override
        public boolean enlistResource(XAResource xaResource)
                throws IllegalStateException, RollbackException, SystemException {
            return false;
        }

        @Override
        public int getStatus() throws SystemException {
            return 0;
        }

        @Override
        public void registerSynchronization(Synchronization synchronization)
                throws IllegalStateException, RollbackException, SystemException {

        }

        @Override
        public void rollback() throws IllegalStateException, SystemException {

        }

        @Override
        public void setRollbackOnly() throws IllegalStateException, SystemException {

        }
    }
}

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
import org.apache.axis2.addressing.EndpointReference;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.engine.AxisConfiguration;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JMSSender.class)
@PowerMockIgnore("javax.management.*")
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
        JMSSender jmsSender = PowerMockito.spy(new JMSSender());
        JMSOutTransportInfo jmsOutTransportInfo = Mockito.mock(JMSOutTransportInfo.class);
        JMSMessageSender jmsMessageSender = Mockito.mock(JMSMessageSender.class);
        Session session = Mockito.mock(Session.class);
        Destination destination = Mockito.mock(Destination.class);

        Mockito.doReturn(session).when(jmsMessageSender).getSession();
        PowerMockito.whenNew(JMSOutTransportInfo.class).withArguments(any(String.class))
                .thenReturn(jmsOutTransportInfo);
        Mockito.doReturn(jmsMessageSender).when(jmsOutTransportInfo).createJMSSender(any(MessageContext.class));
        PowerMockito.doReturn(destination)
                .when(jmsSender, "sendOverJMS", ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                        ArgumentMatchers.any(), ArgumentMatchers.any());

        jmsSender.init(new ConfigurationContext(new AxisConfiguration()), new TransportOutDescription("jms"));
        MessageContext messageContext = new MessageContext();
        //append the transport.jms.TransactionCommand
        String targetAddress = "jms:/SimpleStockQuoteService?transport.jms.ConnectionFactoryJNDIName="
                + "QueueConnectionFactory&transport.jms.TransactionCommand=begin"
                + "&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory";
        Transaction transaction = new TestJMSTransaction();
        messageContext.setProperty(JMSConstants.JMS_XA_TRANSACTION, transaction);

        jmsSender.sendMessage(messageContext, targetAddress, null);
        Map<Transaction, ArrayList<JMSMessageSender>> jmsMessageSenderMap = Whitebox
                .getInternalState(JMSSender.class, "jmsMessageSenderMap");
        Assert.assertEquals("Transaction not added to map", 1, jmsMessageSenderMap.size());
        List senderList = jmsMessageSenderMap.get(transaction);
        Assert.assertNotNull("List is null", senderList );
        Assert.assertEquals("List is empty", 1, senderList.size());
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
        String contentTypePropertyResult = jmsSender.getContentTypeProperty(ctx, jmsOutTransportInfo, factory);
        Assert.assertEquals(contentTypeProperty, contentTypePropertyResult);
    }

    /**
     * This will verify the fixes for generating queues per request when reply to queue is not mentioned in the jms url
     * @throws Exception
     */
    public void testTempQueueCreationForResponse() throws Exception {

        JMSSender jmsSender = PowerMockito.spy(new JMSSender());
        JMSOutTransportInfo jmsOutTransportInfo = Mockito.mock(JMSOutTransportInfo.class);
        JMSMessageSender jmsMessageSender = Mockito.mock(JMSMessageSender.class);
        Session session = Mockito.mock(Session.class);
        Destination destination = Mockito.mock(Destination.class);
        Message message = Mockito.mock(Message.class);
        PowerMockito.doReturn(message)
                .when(jmsSender, "createJMSMessage", ArgumentMatchers.any(), ArgumentMatchers.any(),
                        ArgumentMatchers.any());
        PowerMockito.doNothing()
                .when(jmsSender, "commitXATransaction", ArgumentMatchers.any());
        Mockito.doReturn(destination).when(jmsOutTransportInfo).getReplyDestination();
        Mockito.doReturn(session).when(jmsMessageSender).getSession();
        PowerMockito.whenNew(JMSOutTransportInfo.class).withArguments(any(String.class))
                .thenReturn(jmsOutTransportInfo);
        Mockito.doReturn(jmsMessageSender).when(jmsOutTransportInfo).createJMSSender(any(MessageContext.class));
        jmsSender.init(new ConfigurationContext(new AxisConfiguration()), new TransportOutDescription("jms"));
        MessageContext messageContext = new MessageContext();
        AxisService axisService = Mockito.mock(AxisService.class);
        Mockito.doReturn("name").when(axisService).getName();
        messageContext.setAxisService(axisService);
        EndpointReference endpointReference = Mockito.mock(EndpointReference.class);
        Mockito.doReturn("transport.jms.TransactionCommand=end").when(endpointReference).toString();
        messageContext.setTo(endpointReference);


        //append the transport.jms.TransactionCommand
        String targetAddress = "jms:/SMSStore?transport.jms.ConnectionFactoryJNDIName=" +
                "QueueConnectionFactory&transport.jms.ConnectionFactory=QueueConnectionFactoryAPIM" +
                "&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory" +
                "&transport.jms.TransactionCommand=begin" +
                "&transport.jms.DestinationType=queue";
        Transaction transaction = new TestJMSTransaction();
        messageContext.setProperty(JMSConstants.JMS_XA_TRANSACTION, transaction);

        jmsSender.sendMessage(messageContext, targetAddress, null);
        Map<Transaction, ArrayList<JMSMessageSender>> jmsMessageSenderMap = Whitebox
                .getInternalState(JMSSender.class, "jmsMessageSenderMap");
        Assert.assertEquals("Transaction not added to map", 2, jmsMessageSenderMap.size());
        List senderList = jmsMessageSenderMap.get(transaction);
        Assert.assertNotNull("List is null", senderList );
        Assert.assertEquals("List is empty", 1, senderList.size());
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

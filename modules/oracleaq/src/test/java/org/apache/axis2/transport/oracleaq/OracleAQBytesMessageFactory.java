/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.axis2.transport.oracleaq;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.axis2.transport.testkit.name.Name;

@Name("bytes")
public class OracleAQBytesMessageFactory implements OracleAQMessageFactory<byte[]> {
    public static final OracleAQBytesMessageFactory INSTANCE = new OracleAQBytesMessageFactory();
    
    private OracleAQBytesMessageFactory() {}

    public Message createMessage(Session session, byte[] data) throws JMSException {
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(data);
        return message;
    }

    public byte[] parseMessage(Message message) throws JMSException {
        BytesMessage bytesMessage = (BytesMessage)message;
        byte[] data = new byte[(int)bytesMessage.getBodyLength()];
        bytesMessage.readBytes(data);
        return data;
    }
}

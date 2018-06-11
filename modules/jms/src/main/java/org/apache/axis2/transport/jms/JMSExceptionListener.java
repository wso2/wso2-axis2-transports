/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.jms;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

/**
 * Custom exception listener to handle failure of cached JMS connections in JMS Connection Factory Instances.
 */
public class JMSExceptionListener implements ExceptionListener {

    private static final Log log = LogFactory.getLog(JMSExceptionListener.class);

    private JMSConnectionFactory jmsConnectionFactory;
    private final int connectionIndex;

    JMSExceptionListener(JMSConnectionFactory jmsConnectionFactory, int connectionIndex) {
        this.jmsConnectionFactory = jmsConnectionFactory;
        this.connectionIndex = connectionIndex;
    }

    @Override
    public void onException(JMSException e) {

        log.warn("Cached connection will be cleared due to JMSException on : " + connectionIndex, e);
        jmsConnectionFactory.clearCachedConnection(connectionIndex);
    }
}

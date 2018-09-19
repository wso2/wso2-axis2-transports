/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.jms.dualchannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

/**
 * Custom exception listener to handle failure of a JMS subscriber connection in the Dual-Channel scenario.
 */
class ReplySubscriptionExceptionListener implements ExceptionListener {

    private static final Log log = LogFactory.getLog(ReplySubscriptionExceptionListener.class);

    private String identifier;

    ReplySubscriptionExceptionListener(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public void onException(JMSException e) {

        synchronized (identifier.intern()) {
            log.warn("Cached subscription will be cleared due to JMSException on : " + identifier, e);
            JMSReplyHandler.getInstance().removeReplySubscription(identifier);
        }
    }
}
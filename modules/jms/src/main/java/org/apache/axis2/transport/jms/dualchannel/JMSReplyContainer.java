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

package org.apache.axis2.transport.jms.dualchannel;


import javax.jms.Message;
import java.util.concurrent.CountDownLatch;

/**
 * DataHolder containing a latch so that the JMSSender can be notified of its response message.
 */
public class JMSReplyContainer {

    /**
     * Latch to signal availability of response message.
     */
    private CountDownLatch countDownLatch;

    /**
     * JMS response message.
     */
    private Message message;

    public JMSReplyContainer(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}
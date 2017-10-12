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
import org.mockito.Mockito;

import javax.jms.Destination;

public class JMSSenderTestCase extends TestCase{

    public void testIsWaitForResponseOrReplyDestinationBothTrue()throws Exception {
        JMSSender jmsSender = new JMSSender();
        Destination destination = Mockito.mock(Destination.class);
        boolean result = jmsSender.isWaitForResponseOrReplyDestination(true, destination);
        Assert.assertTrue(result);
    }

    public void testIsWaitForResponseOrReplyDestinationWhenOneIsTrue()throws Exception {
        JMSSender jmsSender = new JMSSender();
        boolean result1 = jmsSender.isWaitForResponseOrReplyDestination(true, null);
        Assert.assertTrue(result1);

        Destination destination = Mockito.mock(Destination.class);
        boolean result2 = jmsSender.isWaitForResponseOrReplyDestination(false, destination);
        Assert.assertTrue(result2);
    }

    public void testIsWaitForResponseOrReplyDestinationWhenBothFalse()throws Exception {
        JMSSender jmsSender = new JMSSender();
        boolean result = jmsSender.isWaitForResponseOrReplyDestination(false, null);
        Assert.assertFalse(result);
    }

}

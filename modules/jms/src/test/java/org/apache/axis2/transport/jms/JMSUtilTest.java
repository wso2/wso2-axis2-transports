/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except 
 * in compliance with the License.
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

/**
 * Unit test for the JMSUtil.java
 */
public class JMSUtilTest extends TestCase {

    private static String url = "jms:/cgc.afid.eproposal.notification?transport.jms" +
            ".ConnectionFactoryJNDIName=QueueConnectionFactory&java.naming.factory.initial=com.tibco.tibjms.naming" +
            ".TibjmsInitialContextFactory&java.naming.provider.url=tcp://localhost:9443&transport.jms" +
            ".DestinationType=queue&transport.jms.UserName=user&transport.jms.Password=secret&java.naming.security.principal=nandika&java.naming.security.credentials=secret";

    /**
     * Testing the URL masking to hide sensitive info.
     */
    public void testUrlMask() {
        String maskedUrl = JMSUtils.maskURLPasswordAndCredentials(url);
        Assert.assertFalse("URL masking test", maskedUrl.contains("secret"));
    }

}

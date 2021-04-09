/*
 *  Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.axis2.transport.jms;

import junit.framework.TestCase;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.Connection;

/**
 * This class is testing the functionality of JMSConnectionFactory.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JMSSender.class)
@PowerMockIgnore("javax.management.*")
public class JMSConnectionFactoryTest extends TestCase {

    @InjectMocks
    JMSConnectionFactory factory = Mockito.mock(JMSConnectionFactory.class);

    private Map<SessionWrapper, Connection> connectionSessionMap = new ConcurrentHashMap<>();
    private Map<Integer, SessionWrapper> sharedSessionWrapperMap = new ConcurrentHashMap<>();

    public void testRemoveInvalidSessions()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Connection connection1 = Mockito.mock(Connection.class);
        Connection connection2 = Mockito.mock(Connection.class);
        SessionWrapper sessionWrapper1 = Mockito.mock(SessionWrapper.class);
        SessionWrapper sessionWrapper2 = Mockito.mock(SessionWrapper.class);

        connectionSessionMap.put(sessionWrapper1, connection1);
        connectionSessionMap.put(sessionWrapper2, connection2);

        sharedSessionWrapperMap.put(1, sessionWrapper1);
        sharedSessionWrapperMap.put(2, sessionWrapper2);

        MockitoAnnotations.initMocks(this);
        ReflectionTestUtils.setField(factory, "sharedSessionWrapperMap", sharedSessionWrapperMap);
        ReflectionTestUtils.setField(factory, "connectionSessionMap", connectionSessionMap);

        Method privateMethod = JMSConnectionFactory.class.getDeclaredMethod("removeInvalidSessions", Connection.class);
        privateMethod.setAccessible(true);
        privateMethod.invoke(factory, connection1);

        assertEquals("Invalid session is not removed", 1, connectionSessionMap.size());
    }
}

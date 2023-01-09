/*
 * Copyright (c) 2023, WSO2 LLC (http://www.wso2.com).
 *
 * WSO2 LLC licenses this file to you under the Apache License,
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

package org.apache.axis2.transport.mail.auth.basic;

import org.apache.axis2.transport.mail.auth.AuthException;
import org.apache.axis2.transport.mail.auth.AuthHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;

/**
 * This class is used to handle Basic Authorization.
 */
public class BasicAuthHandler implements AuthHandler {

    private final String username;
    private final String password;
    private static final Log log = LogFactory.getLog(BasicAuthHandler.class);

    public BasicAuthHandler(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public Store connect(Session session, String protocol) throws AuthException {
        Store store = null;
        log.debug("Connecting to email server via basic auth protocol");
        try {
            store = session.getStore(protocol);
            store.connect(username, password);
            return store;
        } catch (MessagingException e) {
            log.error("An error occurred while trying to connect to mail server for " + username + " via " +
                    protocol + " protocol.");
            throw new AuthException(e.getMessage(), e);
        }
    }
}

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
package org.apache.axis2.transport.mail.auth;

import org.apache.axis2.transport.mail.PollTableEntry;
import org.apache.axis2.transport.mail.auth.basic.BasicAuthHandler;
import org.apache.axis2.transport.mail.auth.oauth.OAuthUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.mail.Session;
import javax.mail.Store;

/**
 * Utility class to build OAuth and Basic handlers
 */
public class AuthUtils {

    private static final Log log = LogFactory.getLog(AuthUtils.class);

    /**
     * This method will connect to mail server based on the auth configs provided.
     * @param entry the PollTableEntry
     * @return the mail store
     * @throws AuthException throw exception upon failure
     */
    public static Store connect(PollTableEntry entry) throws AuthException {
        AuthHandler authHandler = getAuthHandler(entry);
        if (authHandler != null) {
            Session session = entry.getSession();
            return authHandler.connect(session, entry.getProtocol());
        } else {
            throw new AuthException("An invalid authHandler is returned.");
        }
    }

    private static AuthHandler getAuthHandler(PollTableEntry entry) throws AuthException {
        String authMode = entry.getAuthMode();
        if (AuthConstants.OAUTH.equalsIgnoreCase(authMode)) {
            return OAuthUtils.getOAuthHandler(entry.getUserName(), entry.getOAuthConfig());
        } else if (AuthConstants.BASIC_AUTH.equalsIgnoreCase(authMode)) {
            return getBasicAuthHandler(entry.getUserName(), entry.getPassword());
        } else {
            throw new AuthException("Provided auth mode : " + authMode + " is invalid.");
        }
    }

    private static AuthHandler getBasicAuthHandler(String userName, String password) throws AuthException {

        if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
            log.error("Invalid basicAuth configurations provided.");
            throw new AuthException("Invalid basicAuth configurations provided.");
        }
        return new BasicAuthHandler(userName, password);
    }

}

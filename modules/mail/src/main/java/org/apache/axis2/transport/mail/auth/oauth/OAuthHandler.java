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

package org.apache.axis2.transport.mail.auth.oauth;

import org.apache.axis2.transport.mail.auth.AuthException;
import org.apache.axis2.transport.mail.auth.AuthHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.mail.Session;
import javax.mail.Store;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This abstract class is to be used by OAuth handlers
 * This class checks validity of tokens, request for tokens and add tokens to in-memory cache
 */
public abstract class OAuthHandler implements AuthHandler {
    private static final Log log = LogFactory.getLog(OAuthHandler.class);

    private final String username;
    private final String clientId;
    private final String clientSecret;
    private final String tokenUrl;
    private final String tokenId;

    protected OAuthHandler(String username, String clientId, String clientSecret, String tokenUrl, String tokenId) {
        this.username = username;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.tokenUrl = tokenUrl;
        this.tokenId = tokenId;
    }

    public Store connect(Session session, String protocol) throws AuthException {
        try {
            Store store = session.getStore(protocol);
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Connecting to mail service for " + username + " via " + protocol + " protocol.");
                }
                store.connect(username, getToken());
                return store;
            } catch (Exception e) {
                log.error("An error occurred while trying to connect to mail server for " + username + " via " +
                        protocol + " protocol.");
                if (e.getMessage().toLowerCase().contains(OAuthConstants.AUTHENTICATE_FAIL_EXCEPTION_MESSAGE)) {
                    removeTokenFromCache();
                    return retryToConnect(store);
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            throw new AuthException(e.getMessage(), e);
        }
    }

    private Store retryToConnect(Store store) throws AuthException {
        log.info("Retrying to connect to mail service upon authentication failure");
        try {
            store.connect(username, getToken());
        } catch (Exception e) {
            log.error("An error occurred while retrying to connect to mail server.");
            throw new AuthException("An error occurred while retrying to connect to mail server", e);
        }
        return store;
    }

    private void removeTokenFromCache() {
        TokenCache.getInstance().removeToken(tokenId);
    }

    private String getToken() throws AuthException {
        TokenCache tokenCache = TokenCache.getInstance();
        Token token = tokenCache.getTokenObject(tokenId);
        if (null == token || StringUtils.isEmpty(token.getAccessToken()) ||
                isAccessTokenExpired(token.getExpiryTime())) {
            token = refreshAccessToken();
            tokenCache.addToken(tokenId, token);
        }
        return token.getAccessToken();
    }

    private boolean isAccessTokenExpired(long expiryTime) {
        if (expiryTime != -1) {
            return (System.currentTimeMillis() >= expiryTime);
        }
        return false;
    }

    private Token refreshAccessToken() throws AuthException {
        log.info("Refreshing access token in mail transport");
        Map<String,String> headers = new HashMap<>();
        headers.put(OAuthConstants.HEADER_CONTENT_TYPE, OAuthConstants.APPLICATION_X_WWW_FORM_URLENCODED);
        try {
            return OAuthClient.generateAccessToken(getTokenUrl(), headers, buildTokenRequestPayload());
        } catch (IOException e) {
            throw new AuthException(e.getCause());
        }
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getTokenUrl() {
        return tokenUrl;
    }

    public String getTokenId() {
        return tokenId;
    }

    protected abstract String buildTokenRequestPayload();

}

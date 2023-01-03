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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;

/**
 * Singleton class to maintain in-memory token cache
 */
public class TokenCache {
    private static final Log log = LogFactory.getLog(TokenCache.class);

    private static TokenCache tokenCache;
    private HashMap<String, Token> tokenMap;

    private TokenCache() {

    }

    public static TokenCache getInstance() {
        if (tokenCache == null) {
            tokenCache = new TokenCache();
        }
        return tokenCache;
    }

    public Token getTokenObject(String id) {
        return getTokenMap().get(id);
    }

    public void addToken(String key, Token token) {
        getTokenMap().put(key, token);
    }

    public void removeToken(String id) {
        getTokenMap().remove(id);
    }

    private HashMap<String, Token> getTokenMap() {
        if (null == tokenMap) {
            tokenMap = new HashMap<>();
        }
        return tokenMap;
    }
}

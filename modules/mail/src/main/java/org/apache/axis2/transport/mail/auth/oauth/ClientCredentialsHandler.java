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

/**
 * This class is used to handle Client Credentials grant oauth.
 */
public class ClientCredentialsHandler extends OAuthHandler {

    private static final Log log = LogFactory.getLog(ClientCredentialsHandler.class);
    private final String scope;

    public ClientCredentialsHandler(String username, String clientId, String clientSecret, String scope,
                                    String tokenUrl, String tokenId) {
        super(username, clientId, clientSecret, tokenUrl, tokenId);
        this.scope = scope;
    }

    @Override
    protected String buildTokenRequestPayload() {
        return OAuthConstants.CLIENT_CRED_GRANT_TYPE +
                OAuthConstants.PARAM_CLIENT_ID + getClientId() +
                OAuthConstants.PARAM_CLIENT_SECRET + getClientSecret() +
                OAuthConstants.SCOPE + scope;
    }
}

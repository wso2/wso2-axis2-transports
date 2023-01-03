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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class to build OAuth handlers
 * Currently this supports only authorization_code and client_credentials grant type
 */
public class OAuthUtils {

    static final Log log = LogFactory.getLog(OAuthUtils.class);
    public static OAuthHandler getOAuthHandler(String userName, OAuthConfig oAuthConfig) throws AuthException {
        String grantType = oAuthConfig.getGrantType();
        switch (grantType) {
            case OAuthConstants.AUTHORIZATION_CODE_GRANT_TYPE :
                return getAuthorizationCodeHandler(userName, oAuthConfig);
            case OAuthConstants.CLIENT_CREDENTIALS_GRANT_TYPE:
                return getClientCredentialsHandler(userName, oAuthConfig);
            default:
                throw new AuthException("Grant type " + grantType + " is invalid");
        }
    }

    private static OAuthHandler getAuthorizationCodeHandler(String userName, OAuthConfig oAuthConfig)
            throws AuthException {
        String clientId = oAuthConfig.getClientId();
        String clientSecret = oAuthConfig.getClientSecret();
        String refreshToken = oAuthConfig.getRefreshToken();
        String tokenUrl = oAuthConfig.getTokenUrl();
        if (StringUtils.isBlank(clientId) || StringUtils.isBlank(clientSecret) ||
                StringUtils.isBlank(refreshToken) || StringUtils.isBlank(tokenUrl)) {
            throw new AuthException("Invalid configurations provided for authorization code grant type.");
        }
        return new AuthorizationCodeHandler(userName, clientId, clientSecret, refreshToken, tokenUrl,
                oAuthConfig.getTokenId());
    }

    private static OAuthHandler getClientCredentialsHandler(String userName, OAuthConfig oAuthConfig) throws AuthException {
        String clientId = oAuthConfig.getClientId();
        String clientSecret = oAuthConfig.getClientSecret();
        String tokenUrl = oAuthConfig.getTokenUrl();
        String scope = oAuthConfig.getScope();
        if (StringUtils.isBlank(clientId) || StringUtils.isBlank(clientSecret) || StringUtils.isBlank(scope) ||
                StringUtils.isBlank(tokenUrl)) {
            throw new AuthException("Invalid configurations provided for client credentials grant type.");
        }
        return new ClientCredentialsHandler(userName, clientId, clientSecret, scope, tokenUrl,
                oAuthConfig.getTokenId());
    }
}

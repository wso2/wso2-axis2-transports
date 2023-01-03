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

/**
 * Utility class defining constants used by the OAuth handlers
 */
public class OAuthConstants {
    public static final String TRANSPORT_MAIL_OAUTH_GRANT_TYPE = "transport.mail.oauth.grantType";
    public static final String TRANSPORT_MAIL_OAUTH_CLIENT_ID = "transport.mail.oauth.clientId";
    public static final String TRANSPORT_MAIL_OAUTH_CLIENT_SECRET = "transport.mail.oauth.clientSecret";
    public static final String TRANSPORT_MAIL_OAUTH_ACCESS_TOKEN = "transport.mail.oauth.accessToken";
    public static final String TRANSPORT_MAIL_OAUTH_REFRESH_TOKEN = "transport.mail.oauth.refreshToken";
    public static final String TRANSPORT_MAIL_OAUTH_SCOPE = "transport.mail.oauth.scope";
    public static final String TRANSPORT_MAIL_OAUTH_TOKEN_URL = "transport.mail.oauth.tokenUrl";
    public static final String AUTHORIZATION_CODE_GRANT_TYPE = "authorization_code";
    public static final String CLIENT_CREDENTIALS_GRANT_TYPE = "client_credentials";

    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";

    public static final String CLIENT_CRED_GRANT_TYPE = "grant_type=client_credentials";
    public static final String REFRESH_TOKEN_GRANT_TYPE = "grant_type=refresh_token";

    // parameters used to build oauth requests
    public static final String PARAM_USERNAME = "&username=";
    public static final String PARAM_PASSWORD = "&password=";
    public static final String PARAM_CLIENT_ID = "&client_id=";
    public static final String PARAM_CLIENT_SECRET = "&client_secret=";
    public static final String PARAM_REFRESH_TOKEN = "&refresh_token=";
    public static final String SCOPE = "&scope=";
    public static final String AMPERSAND = "&";
    public static final String EQUAL_MARK = "=";

    // elements in the oauth response
    public static final String ACCESS_TOKEN = "access_token";
    public static final String EXPIRES_IN = "expires_in";

    public static final String AUTHENTICATE_FAIL_EXCEPTION_MESSAGE = "authenticate failed";

}

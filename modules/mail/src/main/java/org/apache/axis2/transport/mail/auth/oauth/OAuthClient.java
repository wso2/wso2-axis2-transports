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
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * This class represents the client used to request and retrieve OAuth tokens
 * from an OAuth server
 */
public class OAuthClient {

    private static final Log log = LogFactory.getLog(OAuthClient.class);

    public static Token generateAccessToken(String url, Map<String, String> headers, String payload)
            throws IOException, AuthException {
        Token token;
        if (log.isDebugEnabled()) {
            log.debug("Initializing token generation request: [token-endpoint] " + url);
        }
        CloseableHttpClient httpClient = getHttpClient();
        HttpPost httpPost = new HttpPost(url);
        try {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                httpPost.setHeader(header.getKey(), header.getValue());
            }

            httpPost.setEntity(new StringEntity(payload));
            CloseableHttpResponse response = httpClient.execute(httpPost);
            token = extractToken(response);
        } finally {
            httpClient.close();
            httpPost.releaseConnection();
        }
        return token;
    }

    private static Token extractToken(CloseableHttpResponse response) throws AuthException, IOException {
        int responseCode = response.getStatusLine().getStatusCode();

        HttpEntity entity = response.getEntity();

        Charset charset = ContentType.getOrDefault(entity).getCharset();
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), charset));
        String inputLine;
        StringBuilder stringBuilder = new StringBuilder();

        while ((inputLine = reader.readLine()) != null) {
            stringBuilder.append(inputLine);
        }

        if (log.isDebugEnabled()) {
            log.debug("Response: [status-code] " + responseCode);
        }

        if (responseCode != HttpStatus.SC_OK) {
            throw new AuthException("Error occurred while accessing the Token URL. "
                    + response.getStatusLine());
        }

        JsonParser parser = new JsonParser();
        JsonObject jsonObject = (JsonObject) parser.parse(stringBuilder.toString());
        if (jsonObject.has(OAuthConstants.ACCESS_TOKEN)) {
            String accessToken = jsonObject.get(OAuthConstants.ACCESS_TOKEN).getAsString();
            if (jsonObject.has(OAuthConstants.EXPIRES_IN)) {
                long expiresIn = jsonObject.get(OAuthConstants.EXPIRES_IN).getAsLong();
                long expiryTime = System.currentTimeMillis() + expiresIn * 1000;
                return new Token(accessToken, expiryTime);
            } else {
                throw new AuthException("Missing key [" + OAuthConstants.EXPIRES_IN + "] " +
                        "in the response from the OAuth server");
            }
        } else {
            throw new AuthException("Missing key [" + OAuthConstants.ACCESS_TOKEN + "] " +
                    "in the response from the OAuth server");
        }
    }

    private static CloseableHttpClient getHttpClient() {
        return HttpClientBuilder.create().build();
    }
}

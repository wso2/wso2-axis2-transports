/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.jms;

import javax.jms.Session;

/**
 * A wrapper class for {@link javax.jms.Session} to identify session objects that are in-used when caching enabled
 */
public class SessionWrapper {

    private Session session;
    private boolean isBusy;

    public SessionWrapper(Session session) {
        this.session = session;
        this.isBusy = false;
    }

    /**
     * The {@link javax.jms.Session} return to the caller
     * @return {@link javax.jms.Session} object
     */
    public Session getSession() {
        return session;
    }

    /**
     * Check whether the session is already in use
     * @return status of the SessionWrapper object true/false
     */
    public boolean isBusy() {
        return isBusy;
    }

    /**
     * Set flag of the SessionWrapper object indicating that the session is in use or completed the task
     * @param busy set the status of the SessionWrapper object true/false
     */
    public void setBusy(boolean busy) {
        isBusy = busy;
    }
}

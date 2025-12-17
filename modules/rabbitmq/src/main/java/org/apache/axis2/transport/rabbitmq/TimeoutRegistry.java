/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.rabbitmq;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry that tracks message IDs that have already timed out.
 *
 * Purpose:
 * - Prevent processing of late responses that arrive after a timeout.
 * - Provide fast membership checks and one-shot consumption of timeout flags.
 *
 * Characteristics:
 * - Backed by a concurrent key set; non-blocking, expected O(1) operations.
 * - No timers or automatic expiry; entries remain until removed or consumed.
 *
 * Usage:
 * 1) markTimedOut(id) when a request times out.
 * 2) Before handling a response, call isTimedOut(id) or consumeIfTimedOut(id).
 * 3) After dropping/handling, call remove(id) if you retained the entry.
 *
 * All methods are safe for concurrent use.
 */
public final class TimeoutRegistry {

    private final Set<String> timedOut = ConcurrentHashMap.newKeySet();

    /**
     * Mark a message as timed out (e.g., from SynapseCallBackReceiver).
     */
    public void markTimedOut(String messageId) {
        if (messageId != null) {
            timedOut.add(messageId);
        }
    }

    /**
     * Check if message already timed out (use in RabbitMqSender before handling response).
     */
    public boolean isTimedOut(String messageId) {
        return messageId != null && timedOut.contains(messageId);
    }

    /**
     * Remove entry (call after you drop or successfully handle).
     */
    public void remove(String messageId) {
        if (messageId != null) {
            timedOut.remove(messageId);
        }
    }

    /**
     * Convenience: if timed out, remove & return true to signal "drop now".
     * Usage:
     * if (registry.consumeIfTimedOut(msgId)) { log.warn(...); return; }
     */
    public boolean consumeIfTimedOut(String messageId) {
        return messageId != null && timedOut.remove(messageId);
    }

    /** Current size (metrics/diagnostics). */
    public int size() {
        return timedOut.size();
    }
}

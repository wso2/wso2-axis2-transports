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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Thread-safe registry of inflight RabbitMQ Threads keyed by message ID.
 * - register: associate a message ID with its Thread.
 * - take: atomically remove and return the Thread for a message ID.
 * - unregister: best-effort removal only if the mapping still points to the same Thread.
 */
public class InflightThreads {
    private static final Log log = LogFactory.getLog(InflightThreads.class);
    // messageId -> Thread
    private final ConcurrentHashMap<String, Thread> threadRegistry = new ConcurrentHashMap<>();

    public void register(String messageId, Thread thread) {
        if (messageId != null && thread != null) threadRegistry.put(messageId, thread);
    }

    /**
     * Remove and return the channel atomically (null if absent).
     */
    public Thread take(String messageId) {
        return (messageId == null) ? null : threadRegistry.remove(messageId);
    }

    /**
     * Atomically: (1) find waiter by messageId, (2) interrupt it, (3) remove it.
     * Returns the interrupted Thread (for logging), or null if none was present.
     */
    public Thread interruptRemoveAndReturn(String messageId) {
        final AtomicReference<Thread> holder = new AtomicReference<>();

        threadRegistry.computeIfPresent(messageId, (key, thread) -> {
            try {
                thread.interrupt();      // sets interrupt flag; waiter will throw InterruptedException
                log.warn("Interrupted executing thread of publisher confirms to cancel publishing for messageId : "
                        + messageId + " Thread Name : " + thread.getName() + " Thread Id : "
                        + thread.getId() + " Thread Group : " + thread.getThreadGroup());
                holder.set(thread);      // capture for logging after we exit the lambda
            } catch (Exception ignore) {
                if (log.isDebugEnabled()) {
                    log.debug("Exception while interrupting the publishing thread for timed-out messageId : "
                            + messageId, ignore);
                }
            }
            return null;  // returning null removes the entry atomically
        });
        return holder.get();
    }
}

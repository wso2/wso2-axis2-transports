/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.Channel;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Pool implementation for the rabbitmq channel associating with the connection factory name
 */
public class RabbitMQChannelPool extends GenericKeyedObjectPool<String, Channel> {

    private static final Log log = LogFactory.getLog(RabbitMQChannelPool.class);
    // Map to store the last time a warning was logged for each key
    private final ConcurrentMap<String, Instant> lastWarningTimeMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Instant> exhaustWarningTimeMap = new ConcurrentHashMap<>();
    private int exhaustWarnInterval = RabbitMQConstants.DEFAULT_CHANNEL_EXHAUST_WARN_INTERVAL;

    public RabbitMQChannelPool(BaseKeyedPooledObjectFactory<String, Channel> rabbitMQChannelFactory,
        int poolSize, RabbitMQConnectionFactory factory) {
        super(rabbitMQChannelFactory);
        this.setTestOnBorrow(true);
        this.setMaxTotal(poolSize);
        this.setMaxIdlePerKey(RabbitMQConstants.DEFAULT_MAX_IDLE_PER_KEY);
        this.setMaxTotalPerKey(RabbitMQConstants.DEFAULT_MAX_IDLE_PER_KEY);
        this.setMaxWaitMillis(RabbitMQConstants.DEFAULT_MAX_WAIT_MILLIS);
        Map<String, String> evictionParams = factory.getConnectionFactoryConfiguration
            (RabbitMQConstants.EVICTION_STRATEGY_PARAMETERS);
        if (evictionParams != null) {
            // For int values
            trySetIntParam(evictionParams, RabbitMQConstants.MAX_IDLE_PER_KEY,
                this::setMaxIdlePerKey);
            trySetIntParam(evictionParams, RabbitMQConstants.MAX_IDLE_PER_KEY,
                this::setMaxTotalPerKey);

            // For long values
            trySetLongParam(evictionParams, RabbitMQConstants.MAX_WAIT_MILLIS,
                this::setMaxWaitMillis);
            trySetLongParam(evictionParams, RabbitMQConstants.MIN_EVICTABLE_IDLE_TIME,
                this::setMinEvictableIdleTimeMillis);
            trySetLongParam(evictionParams, RabbitMQConstants.TIME_BETWEEN_EVICTION_RUNS
                , this::setTimeBetweenEvictionRunsMillis);
            this.setTestWhileIdle(true); // Optionally, test objects for validity while idle
            if (evictionParams.containsKey(RabbitMQConstants.CHANNEL_EXHAUST_WARN_INTERVAL)) {
                try {
                    exhaustWarnInterval = Integer.parseInt(evictionParams.get(
                        RabbitMQConstants.CHANNEL_EXHAUST_WARN_INTERVAL));
                } catch (NumberFormatException e) {
                    log.warn("Invalid value for " + RabbitMQConstants.CHANNEL_EXHAUST_WARN_INTERVAL
                        + " : " + evictionParams.get(
                        RabbitMQConstants.CHANNEL_EXHAUST_WARN_INTERVAL));
                }
            }
        }
    }

    // Helper method for int parameters
    private void trySetIntParam(Map<String, String> params, String key, Consumer<Integer> setter) {
        String value = params.get(key);
        if (value != null) {
            try {
                setter.accept(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                log.warn("Invalid value for " + key + " : " + value);
            }
        }
    }

    // Helper method for long parameters
    private void trySetLongParam(Map<String, String> params, String key, Consumer<Long> setter) {
        String value = params.get(key);
        if (value != null) {
            try {
                setter.accept(Long.parseLong(value));
            } catch (NumberFormatException e) {
                log.warn("Invalid value for " + key + " : " + value);
            }
        }
    }


    /**
     * Obtains an instance from the pool for the specified key
     *
     * @param factoryName pool key
     * @return a {@link Channel} object
     * @throws Exception
     */
    @Override
    public Channel borrowObject(String factoryName) throws Exception {
        try {
            int maxCapacity = getMaxTotalPerKey();
            int warningThreshold = (int) (maxCapacity * 0.75);
            int activeChannels = getNumActive(factoryName);
            if (activeChannels >= warningThreshold) {
                lastWarningTimeMap.compute(factoryName, (k, lastWarningTime) -> {
                    Instant now = Instant.now();
                    if (lastWarningTime == null ||
                        lastWarningTime.plus(exhaustWarnInterval, ChronoUnit.SECONDS).isBefore(now)) {
                        // Log a warning if the number of active channels exceeds 75% of the max capacity
                        log.warn("RabbitMQ channel pool for key " + factoryName
                            + " is nearing capacity. Currently at " + activeChannels
                            + "/" + getMaxIdlePerKey() + " channels. This message will not be "
                            + "repeated for " + exhaustWarnInterval + " second(s)");
                        return now;
                    }
                    return lastWarningTime; // Return the existing time to not update the map
                });
            }
            if (activeChannels == maxCapacity) {
                exhaustWarningTimeMap.compute(factoryName, (k, lastExhaustWarningTime) -> {
                    Instant now = Instant.now();
                    if (lastExhaustWarningTime == null ||
                        lastExhaustWarningTime.plus(exhaustWarnInterval, ChronoUnit.SECONDS).isBefore(now)) {
                        // Log a warning if the pool is exhausted
                        log.warn("RabbitMQ channel pool for key " + factoryName
                            + " is [EXHAUSTED]. This message will not be repeated for "
                            + exhaustWarnInterval + " second(s)");
                        return now;
                    }
                    return lastExhaustWarningTime; // Return the existing time to not update the map
                });
            }
            return super.borrowObject(factoryName);
        } catch (NoSuchElementException nse) {
            if (nse.getMessage().contains("Timeout waiting for idle object")) {
                throw new AxisRabbitMQException("Timeout occurred while waiting for a channel of "
                    + factoryName + " from the pool. Pool [EXHAUSTED]", nse);
            }
            // The exception was caused by an exhausted pool
            if (null == nse.getCause()) {
                throw new AxisRabbitMQException("Error occurred while getting a channel of " + factoryName +
                        " since the pool is exhausted", nse);
            }
            // Otherwise, the exception was caused by the implemented activateObject() or validateObject()
            throw new AxisRabbitMQException("Error occurred while borrowing a channel of " + factoryName, nse);
        }
    }

    /**
     * Returns a channel to a keyed pool if it opens. Otherwise, destroy the channel
     *
     * @param factoryName pool key
     * @param channel     instance to return to the keyed pool
     */
    @Override
    public void returnObject(String factoryName, Channel channel) {
        try {
            if (channel != null) {
                if (channel.isOpen()) {
                    super.returnObject(factoryName, channel);
                } else {
                    super.invalidateObject(factoryName, channel);
                }
            }
        } catch (Exception e) {
            log.error("Error occurred while returning a channel of " + factoryName + " back to the pool", e);
        }
    }
}

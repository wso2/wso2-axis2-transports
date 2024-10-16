/**
 *  Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * The {@code RabbitMQConfigUtils} class is responsible for retrieving
 * RabbitMQ configuration properties, including throttle limit,
 * throttle mode, and throttle time unit.
 */
public class RabbitMQConfigUtils {

    private static final Log log = LogFactory.getLog(RabbitMQConfigUtils.class);

    /**
     *
     * Retrieves the throttle count from rabbitMQProperties.
     *
     * @param rabbitMQProperties the map containing the configuration properties
     * @return the throttle limit, defaulting to 60
     */
    public static int getThrottleCount(Map<String, String> rabbitMQProperties) {
        String throttleCountStr = rabbitMQProperties.get(RabbitMQConstants.RABBITMQ_PROXY_THROTTLE_COUNT);
        int throttleCount = RabbitMQConstants.RABBITMQ_PROXY_DEFAULT_THROTTLE_LIMIT; // Default value

        if (throttleCountStr != null) {
            try {
                throttleCount = Integer.parseInt(throttleCountStr);
                if (throttleCount <= 0) {
                    log.error("Throttle limit value is zero or negative, using default: "
                            + RabbitMQConstants.RABBITMQ_PROXY_DEFAULT_THROTTLE_LIMIT);
                    throttleCount = RabbitMQConstants.RABBITMQ_PROXY_DEFAULT_THROTTLE_LIMIT;
                }
            } catch (NumberFormatException e) {
                log.error("Invalid throttle limit value '" + throttleCountStr + "', using default: "
                        + RabbitMQConstants.RABBITMQ_PROXY_DEFAULT_THROTTLE_LIMIT, e);
            }
        } else {
            log.warn("Throttle count property is not set, using default: "
                    + RabbitMQConstants.RABBITMQ_PROXY_DEFAULT_THROTTLE_LIMIT);
        }

        return throttleCount;
    }

    /**
     * Retrieves the throttle mode from rabbitMQProperties.
     *
     * @param rabbitMQProperties the map containing the configuration properties
     * @return the throttle mode as a string, defaulting to FIXED_INTERVAL
     */
    public static RabbitMQConstants.ThrottleMode getThrottleMode(Map<String, String> rabbitMQProperties) {
        String throttleModeStr = rabbitMQProperties.get(RabbitMQConstants.RABBITMQ_PROXY_THROTTLE_MODE);
        RabbitMQConstants.ThrottleMode throttleMode = null;

        if (throttleModeStr != null) {
            throttleMode = RabbitMQConstants.ThrottleMode.fromString(throttleModeStr);
        }

        if (throttleMode == null) {
            log.error("Invalid or missing throttle mode: " + throttleModeStr + ", using default: "
                    + RabbitMQConstants.ThrottleMode.FIXED_INTERVAL);
            throttleMode = RabbitMQConstants.ThrottleMode.FIXED_INTERVAL; // default mode
        }

        return throttleMode;
    }

    /**
     * Retrieves the throttle time unit from rabbitMQProperties.
     *
     * @param rabbitMQProperties the map containing the configuration properties
     * @return the throttle time unit as a string, defaulting to MINUTE
     */
    public static RabbitMQConstants.ThrottleTimeUnit getThrottleTimeUnit(Map<String, String> rabbitMQProperties) {
        String timeUnitStr = rabbitMQProperties.get(RabbitMQConstants.RABBITMQ_PROXY_THROTTLE_TIME_UNIT);
        RabbitMQConstants.ThrottleTimeUnit timeUnit = null;

        if (timeUnitStr != null) {
            timeUnit = RabbitMQConstants.ThrottleTimeUnit.fromString(timeUnitStr);
        }

        if (timeUnit == null) {
            log.error("Invalid or missing throttle time unit: " + timeUnitStr + ", using default: "
                    + RabbitMQConstants.ThrottleTimeUnit.MINUTE);
            timeUnit = RabbitMQConstants.ThrottleTimeUnit.MINUTE;
        }

        return timeUnit;
    }
}


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
import org.wso2.config.mapper.ConfigParser;

import java.util.Map;

/**
 * Centralized, loader for RabbitMQ callback/ack related configuration.
 * <p>
 * Values are parsed once at class-load time from {@link ConfigParser#getParsedConfigs()} and
 * exposed via simple getters. Defaults are applied on missing/invalid values with logs.
 *
 * <p>Keys (under {@code RabbitMQConstants.MESSAGING_CALLBACK_CONFIGS} namespace):
 * <ul>
 *   <li>{@code RabbitMQConstants.RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME} (long, ms)</li>
 *   <li>{@code RabbitMQConstants.DEFAULT_PUBLISHER_CONFIRMS_WAIT_TIMEOUT_WHEN_CALLBACK_ENABLED} (long, ms)</li>
 *   <li>{@code RabbitMQConstants.CALLBACK_CONTROLLED_ACK} (boolean)</li>
 *   <li>{@code RabbitMQConstants.AVOID_DECLARING_EXCHANGE_QUEUE} (boolean)</li>
 * </ul>
 */
public class RabbitMQAckConfig {
    private static final Log log = LogFactory.getLog(RabbitMQAckConfig.class);

    private static long inboundAckMaxWaitTimeMs;
    private static final long defaultPubConfirmsTimeoutWhenCallbackEnabledMs;
    private static final boolean callbackControlledAckEnabled;
    private static final boolean avoidDeclaringExchangesQueuesWhenPublishing;

    static {
        Map<String, Object> configs = ConfigParser.getParsedConfigs();
        String baseForCallBackConfigs = RabbitMQConstants.MESSAGING_CALLBACK_CONFIGS + ".";

        // Temp holders
        long tmpInboundAckMaxWaitTime;
        long tmpPublisherConfirmsTimeout;
        boolean tmpCallbackControlledAck;
        boolean tmpAvoidDeclares;

        // Inbound ACK max wait time
        Object inboundAckMaxWaitTime = configs.get(baseForCallBackConfigs
                + RabbitMQConstants.RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME);
        if (inboundAckMaxWaitTime != null) {
            try {
                tmpInboundAckMaxWaitTime = Long.parseLong(inboundAckMaxWaitTime.toString());
                log.info("Using RabbitMQ Inbound Ack Max Wait Time value of : " + tmpInboundAckMaxWaitTime + " ms");
            } catch (NumberFormatException e) {
                log.warn("Invalid value for " + RabbitMQConstants.RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME
                        + ". Should be a valid long value. Defaulting to "
                        + RabbitMQConstants.RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME_DEFAULT);
                tmpInboundAckMaxWaitTime = RabbitMQConstants.RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME_DEFAULT;
            }
        } else {
            log.info("A value hasn't been set to " + RabbitMQConstants.RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME
                    + ". Defaulting to " + RabbitMQConstants.RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME_DEFAULT);
            tmpInboundAckMaxWaitTime = RabbitMQConstants.RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME_DEFAULT;
        }

        // Default publisher confirms timeout (when callback-ack enabled)
        Object defaultPubConfirms = configs.get(baseForCallBackConfigs +
                RabbitMQConstants.DEFAULT_PUB_CON_WAIT_TIMEOUT_WHEN_CALLBACK_ENABLED);
        if (defaultPubConfirms != null) {
            try {
                tmpPublisherConfirmsTimeout = Long.parseLong(defaultPubConfirms.toString());
                log.info("Using default publisher confirm timeout (callback-ack enabled) value of : "
                        + tmpPublisherConfirmsTimeout + " ms");
            } catch (NumberFormatException e) {
                log.warn("Invalid value for "
                        + RabbitMQConstants.DEFAULT_PUB_CON_WAIT_TIMEOUT_WHEN_CALLBACK_ENABLED
                        + ". Should be a valid long value. Defaulting to "
                        + RabbitMQConstants.DEFAULT_VALUE_FOR_PUBCON_WAIT_TIMEOUT_WHEN_CALLBACK);
                tmpPublisherConfirmsTimeout =
                        RabbitMQConstants.DEFAULT_VALUE_FOR_PUBCON_WAIT_TIMEOUT_WHEN_CALLBACK;
            }
        } else {
            log.info("A value hasn't been set to "
                    + RabbitMQConstants.DEFAULT_PUB_CON_WAIT_TIMEOUT_WHEN_CALLBACK_ENABLED
                    + ". Defaulting to "
                    + RabbitMQConstants.DEFAULT_VALUE_FOR_PUBCON_WAIT_TIMEOUT_WHEN_CALLBACK);
            tmpPublisherConfirmsTimeout =
                    RabbitMQConstants.DEFAULT_VALUE_FOR_PUBCON_WAIT_TIMEOUT_WHEN_CALLBACK;
        }

        // Callback controlled ack enabled
        Object callbackAck = configs.get(baseForCallBackConfigs + RabbitMQConstants.CALLBACK_CONTROLLED_ACK);
        if (callbackAck != null) {
            try {
                tmpCallbackControlledAck = Boolean.parseBoolean(callbackAck.toString());
                log.info("Callback Controlled ack is set to : " + tmpCallbackControlledAck);
            } catch (NumberFormatException e) {
                log.warn("Invalid value for " + RabbitMQConstants.CALLBACK_CONTROLLED_ACK
                        + ". Should be true or false. Defaulting to false");
                tmpCallbackControlledAck = false;
            }
        } else {
            log.info("A value hasn't been set to " + RabbitMQConstants.CALLBACK_CONTROLLED_ACK
                    + ". Defaulting to false");
            tmpCallbackControlledAck = false;
        }

        // Avoid declaring exchanges/queues when publishing
        Object avoidDeclares = configs.get(baseForCallBackConfigs + RabbitMQConstants.AVOID_DECLARING_EXCHANGE_QUEUE);
        if (avoidDeclares != null) {
            try {
                tmpAvoidDeclares = Boolean.parseBoolean(avoidDeclares.toString());
                log.info("Avoid Declaring Queues and Exchanges is set to : " + tmpAvoidDeclares);
            } catch (NumberFormatException e) {
                log.warn("Invalid value for " + RabbitMQConstants.AVOID_DECLARING_EXCHANGE_QUEUE
                        + ". Should be true or false. Defaulting to false");
                tmpAvoidDeclares = false;
            }
        } else {
            log.info("A value hasn't been set to " + RabbitMQConstants.AVOID_DECLARING_EXCHANGE_QUEUE
                    + ". Defaulting to false");
            tmpAvoidDeclares = false;
        }

        // Assign to final fields
        inboundAckMaxWaitTimeMs = tmpInboundAckMaxWaitTime;
        defaultPubConfirmsTimeoutWhenCallbackEnabledMs = tmpPublisherConfirmsTimeout;
        callbackControlledAckEnabled = tmpCallbackControlledAck;
        avoidDeclaringExchangesQueuesWhenPublishing = tmpAvoidDeclares;
    }

    private RabbitMQAckConfig() {
        // Prevent instantiation
    }

    public static long getInboundAckMaxWaitTimeMs() {
        return inboundAckMaxWaitTimeMs;
    }

    public static void setInboundAckMaxWaitTimeMs(long inboundAckMaxWaitTime) {
        inboundAckMaxWaitTimeMs = inboundAckMaxWaitTime;
    }

    public static long getDefaultPublisherConfirmsTimeoutWhenCallbackEnabledMs() {
        return defaultPubConfirmsTimeoutWhenCallbackEnabledMs;
    }

    public static boolean isCallbackControlledAckEnabled() {
        return callbackControlledAckEnabled;
    }

    public static boolean isAvoidDeclaringExchangesQueuesWhenPublishing() {
        return avoidDeclaringExchangesQueuesWhenPublishing;
    }
}

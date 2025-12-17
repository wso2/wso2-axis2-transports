/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.rabbitmq.client.BuiltinExchangeType;

/**
 * Holds constants for rabbitmq transport
 */
public class RabbitMQConstants {
    public static final String AMQ_PREFIX = "amq.";
    public static final String CONTENT_TYPE_PROPERTY_PARAM = "rabbitmq.transport.ContentTypeProperty";
    public static final String RABBITMQ_REPLY_TO = "RABBITMQ_REPLY_TO";
    public static final String RABBITMQ_WAIT_REPLY = "RABBITMQ_WAIT_REPLY";
    public static final String RABBITMQ_WAIT_REPLY_TIMEOUT = "rabbitmq.wait.reply.timeout";
    public static final String RABBITMQ_WAIT_CONFIRMS = "RABBITMQ_WAIT_CONFIRMS";
    public static final String RABBITMQ_PUBLISHER_CONFIRMS_WAIT_TIMEOUT = "rabbitmq.publisher.confirms.wait.timeout";
    public static final String SOAP_ACTION = "SOAP_ACTION";
    public static final String SET_REQUEUE_ON_ROLLBACK = "SET_REQUEUE_ON_ROLLBACK";
    public static final String MESSAGING_CALLBACK_CONFIGS = "messaging_callback";
    public static final String CALLBACK_CONTROLLED_ACK = "callback_controlled_ack_enabled";
    public static final String AVOID_DECLARING_EXCHANGE_QUEUE = "avoid_declaring_exchanges_queues";

    public static final String RABBITMQ_CON_FAC = "rabbitmq.connection.factory";

    public static final String SERVER_HOST_NAME = "rabbitmq.server.host.name";
    public static final String SERVER_PORT = "rabbitmq.server.port";
    public static final String SERVER_USER_NAME = "rabbitmq.server.user.name";
    public static final String SERVER_PASSWORD = "rabbitmq.server.password";
    public static final String SERVER_VIRTUAL_HOST = "rabbitmq.server.virtual.host";

    public static final String HEARTBEAT = "rabbitmq.connection.factory.heartbeat";
    public static final String CONNECTION_TIMEOUT = "rabbitmq.connection.factory.timeout";
    public static final String HANDSHAKE_TIMEOUT = "rabbitmq.connection.factory.handshake.timeout";
    public static final String NETWORK_RECOVERY_INTERVAL = "rabbitmq.connection.factory.network.recovery.interval";
    public static final String RETRY_INTERVAL = "rabbitmq.connection.retry.interval";
    public static final String RETRY_COUNT = "rabbitmq.connection.retry.count";
    public static final String MAX_INBOUND_MESSAGE_BODY_SIZE = "rabbitmq.max.inbound.message.body.size";

    public static final String CORRELATION_ID = "rabbitmq.message.correlation.id";
    public static final String MESSAGE_ID = "rabbitmq.message.id";
    public static final String REPLY_TO_QUEUE_NAME = "rabbitmq.replyto.name";
    public static final String CONTENT_TYPE = "rabbitmq.message.content.type";
    public static final String CONTENT_ENCODING = "rabbitmq.message.content.encoding";
    public static final String EXPIRATION = "rabbitmq.message.expiration";
    public static final String TIME_STAMP = "rabbitmq.message.timestamp";
    public static final String USER_ID = "rabbitmq.message.user.id";
    public static final String APP_ID = "rabbitmq.message.app.id";
    public static final String CLUSTER_ID = "rabbitmq.message.cluster.id";
    public static final String MSG_PRIORITY = "rabbitmq.message.priority";
    public static final String MESSAGE_TYPE = "rabbitmq.message.type";

    public static final String QUEUE_AUTODECLARE = "rabbitmq.queue.autodeclare";
    public static final String EXCHANGE_AUTODECLARE = "rabbitmq.exchange.autodeclare";

    // Authentication Mechanisms auth_mechanisms
    public static final String EXTERNAL_AUTH_MECHANISM = "rabbitmq.auth.mechanism.external";

    //SSL related properties
    public static final String SSL_ENABLED = "rabbitmq.connection.ssl.enabled";
    public static final String SSL_KEYSTORE_LOCATION = "rabbitmq.connection.ssl.keystore.location";
    public static final String SSL_KEYSTORE_TYPE = "rabbitmq.connection.ssl.keystore.type";
    public static final String SSL_KEYSTORE_PASSWORD = "rabbitmq.connection.ssl.keystore.password";
    public static final String SSL_TRUSTSTORE_LOCATION = "rabbitmq.connection.ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_TYPE = "rabbitmq.connection.ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_PASSWORD = "rabbitmq.connection.ssl.truststore.password";
    public static final String SSL_VERSION = "rabbitmq.connection.ssl.version";

    public static final String EXCHANGE_NAME = "rabbitmq.exchange.name";
    public static final String EXCHANGE_TYPE = "rabbitmq.exchange.type";
    public static final String EXCHANGE_DURABLE = "rabbitmq.exchange.durable";
    public static final String EXCHANGE_AUTO_DELETE = "rabbitmq.exchange.auto.delete";
    public static final String EXCHANGE_OPTIONAL_ARG_PREFIX = "rabbitmq.exchange.optional.";
    public static final String INBOUND_ACK_MAX_WAIT_PARAM = "rabbitmq.inbound.ack.max.wait.time";

    public static final String QUEUE_NAME = "rabbitmq.queue.name";
    public static final String QUEUE_DURABLE = "rabbitmq.queue.durable";
    public static final String QUEUE_EXCLUSIVE = "rabbitmq.queue.exclusive";
    public static final String QUEUE_AUTO_DELETE = "rabbitmq.queue.auto.delete";
    public static final String QUEUE_AUTO_ACK = "rabbitmq.queue.auto.ack";
    public static final String QUEUE_ROUTING_KEY = "rabbitmq.queue.routing.key";
    public static final String QUEUE_DELIVERY_MODE = "rabbitmq.queue.delivery.mode"; // 1=NonPersistent , 2=Persistent
    public static final String QUEUE_OPTIONAL_ARG_PREFIX = "rabbitmq.queue.optional.";

    public static final String CONSUMER_QOS = "rabbitmq.channel.consumer.qos";
    public static final String CONSUMER_TAG = "rabbitmq.consumer.tag";
    public static final String CONCURRENT_CONSUMER_COUNT = "rabbitmq.concurrent.consumer.count";

    public static final String MESSAGE_MAX_DEAD_LETTERED_COUNT = "rabbitmq.message.max.dead.lettered.count";
    public static final String MESSAGE_ERROR_EXCHANGE_NAME = "rabbitmq.message.error.exchange.name";
    public static final String MESSAGE_ERROR_QUEUE_ROUTING_KEY = "rabbitmq.message.error.queue.routing.key";
    public static final String MESSAGE_REQUEUE_DELAY = "rabbitmq.message.requeue.delay";

    public static final String PUBLISHER_CONFIRMS_ENABLED = "rabbitmq.publisher.confirms.enabled";
    public static final String UNDEPLOYMENT_GRACE_TIMEOUT = "rabbitmq.undeployment.grace.timeout";

    public static final String DEFAULT_CONTENT_TYPE = "text/plain";
    public static final int DEFAULT_RETRY_INTERVAL = 30000;
    public static final int DEFAULT_RETRY_COUNT = 3;
    public static final int DEFAULT_DELIVERY_MODE = 2; //Default is persistent
    public static final int DEFAULT_RABBITMQ_TIMEOUT = 30000;
    public static final int DEFAULT_POOL_SIZE = 200;
    public static final int DEFAULT_CONSUMER_QOS = 0;

    /** Default concurrent consumer count */
    public static final int CONCURRENT_CONSUMER_COUNT_DEFAULT = 1;
    
    // This is because, anyway the timeout handler will close the channel on endpoint timeout
    public static final long DEFAULT_VALUE_FOR_PUBCON_WAIT_TIMEOUT_WHEN_CALLBACK = 180000;
    public static final String DEFAULT_PUB_CON_WAIT_TIMEOUT_WHEN_CALLBACK_ENABLED =
            "default_pub_confirm_timeout_for_callback_controlled_flow";
    public static String RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME = "rabbitmq_inbound_ack_max_wait_ms";
    public static final long RABBITMQ_INBOUND_ACK_MAX_WAIT_TIME_DEFAULT = 180000;
    public static final String ACKNOWLEDGEMENT_DECISION = "ACK_DECISION";

    public static final String SECURE_VAULT_NAMESPACE = "http://org.wso2.securevault/configuration";
    public static final String SECRET_ALIAS_ATTRIBUTE = "secretAlias";
    public static final String PARAM_POOL_SIZE = "poolSize";

    public static final String EVICTION_STRATEGY_PARAMETERS = "EVICTION_STRATEGY_PARAMETERS";

    public static final String MIN_EVICTABLE_IDLE_TIME = "rabbitmq.min.evictable.idle.time";
    public static final String TIME_BETWEEN_EVICTION_RUNS = "rabbitmq.time.between.eviction.runs";
    public static final String MAX_IDLE_PER_KEY = "rabbitmq.max.idle.per.key";
    public static final int DEFAULT_MAX_IDLE_PER_KEY = 100;
    public static final String CHANNEL_EXHAUST_WARN_INTERVAL = "rabbitmq.channel.exhaust.warn.interval";
    public static final int DEFAULT_CHANNEL_EXHAUST_WARN_INTERVAL = 30; // in seconds
    public static final String MAX_WAIT_MILLIS = "rabbitmq.max.wait.millis";
    public static final int DEFAULT_MAX_WAIT_MILLIS = 180000;
    public static final String CONNECTION_POOL_SIZE = "rabbitmq.connection.pool.size";

    public static final String EXCHANGE_TYPE_DEFAULT = BuiltinExchangeType.DIRECT.getType();
    public static final String EXCHANGE_DURABLE_DEFAULT = "true";
    public static final String QUEUE_DURABLE_DEFAULT = "true";

    public static final String IN_EQUIVALENT_ARGUMENT_ERROR = "inequivalent arg";
    public static final String QUEUE = "queue";
    public static final String EXCHANGE = "exchange";

    /**
     * Throttling related constants
     */
    public static final String RABBITMQ_PROXY_THROTTLE_ENABLED = "rabbitmq.proxy.throttle.enabled";
    public static final String RABBITMQ_PROXY_THROTTLE_MODE = "rabbitmq.proxy.throttle.mode";
    public static final String RABBITMQ_PROXY_THROTTLE_TIME_UNIT = "rabbitmq.proxy.throttle.timeUnit";
    public static final String RABBITMQ_PROXY_THROTTLE_COUNT = "rabbitmq.proxy.throttle.count";
    public static final int RABBITMQ_PROXY_DEFAULT_THROTTLE_LIMIT = 60;

    /**
     * Throttle mode
     */
    public enum ThrottleMode {
        FIXED_INTERVAL,
        BATCH;

        public static ThrottleMode fromString(String mode) {
            try {
                return ThrottleMode.valueOf(mode.toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }
    /**
     * Throttle time unit
     */
    public enum ThrottleTimeUnit {
        MINUTE,
        HOUR,
        DAY;

        public static ThrottleTimeUnit fromString(String unit) {
            try {
                return ThrottleTimeUnit.valueOf(unit.toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

}

/*
* Copyright 2004,2005 The Apache Software Foundation.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.axis2.transport.jms;

import org.apache.axis2.client.Options;

import javax.xml.namespace.QName;

public class JMSConstants {

    /**
     * The prefix indicating an Axis JMS URL
     */
    public static final String JMS_PREFIX = "jms:/";

    //------------------------------------ defaults / constants ------------------------------------
    /**
     * The local (Axis2) JMS connection factory name of the default connection
     * factory to be used, if a service does not explicitly state the connection
     * factory it should be using by a Parameter named JMSConstants.CONFAC_PARAM
     */
    public static final String DEFAULT_CONFAC_NAME = "default";
    /**
     * The default JMS time out waiting for a reply - also see {@link JMS_WAIT_REPLY}
     */
    public static final long DEFAULT_JMS_TIMEOUT = Options.DEFAULT_TIMEOUT_MILLISECONDS;
    /**
     * Value indicating a Queue used for {@link DEST_PARAM_TYPE}, {@link REPLY_PARAM_TYPE}
     */
    public static final String DESTINATION_TYPE_QUEUE = "queue";
    /**
     * Value indicating a Topic used for {@link DEST_PARAM_TYPE}, {@link REPLY_PARAM_TYPE}
     */
    public static final String DESTINATION_TYPE_TOPIC = "topic";
    /**
     * Value indicating a JMS 1.1 Generic Destination used by {@link DEST_PARAM_TYPE}, {@link REPLY_PARAM_TYPE}
     */
    public static final String DESTINATION_TYPE_GENERIC = "generic";

    /** Do not cache any JMS resources between tasks (when sending) or JMS CF's (when sending) */
	public static final int CACHE_NONE = 0;
	/** Cache only the JMS connection between tasks (when receiving), or JMS CF's (when sending)*/
	public static final int CACHE_CONNECTION = 1;
	/** Cache only the JMS connection and Session between tasks (receiving), or JMS CF's (sending) */
	public static final int CACHE_SESSION = 2;
	/** Cache the JMS connection, Session and Consumer between tasks when receiving*/
	public static final int CACHE_CONSUMER = 3;
	/** Cache the JMS connection, Session and Producer within a JMSConnectionFactory when sending */
	public static final int CACHE_PRODUCER = 4;
    /** automatic choice of an appropriate caching level (depending on the transaction strategy) */
	public static final int CACHE_AUTO = 5;

    /** A JMS 1.1 Generic Destination type or ConnectionFactory */
    public static final int GENERIC = 0;
    /** A Queue Destination type or ConnectionFactory */
    public static final int QUEUE = 1;
    /** A Topic Destination type or ConnectionFactory */
    public static final int TOPIC = 2;

    /**
     * The EPR parameter name indicating the name of the message level property that indicated the content type.
     */
    public static final String CONTENT_TYPE_PROPERTY_PARAM = "transport.jms.ContentTypeProperty";

    //---------------------------------- services.xml parameters -----------------------------------
    /**
     * The Service level Parameter name indicating the JMS destination for requests of a service
     */
    public static final String PARAM_DESTINATION = "transport.jms.Destination";
    /**
     * The Service level Parameter name indicating the destination type for requests.
     * also see {@link DESTINATION_TYPE_QUEUE}, {@link DESTINATION_TYPE_TOPIC}
     */
    public static final String PARAM_DEST_TYPE = "transport.jms.DestinationType";
    /**
     * The Service level Parameter name indicating the [default] response destination of a service
     */
    public static final String PARAM_REPLY_DESTINATION = "transport.jms.ReplyDestination";
    /**
     * The Service level Parameter name indicating the response destination type
     * also see {@link DESTINATION_TYPE_QUEUE}, {@link DESTINATION_TYPE_TOPIC}
     */
    public static final String PARAM_REPLY_DEST_TYPE = "transport.jms.ReplyDestinationType";
    /**
     * The Parameter name of an Axis2 service, indicating the JMS connection
     * factory which should be used to listen for messages for it. This is
     * the local (Axis2) name of the connection factory and not the JNDI name
     */
    public static final String PARAM_JMS_CONFAC = "transport.jms.ConnectionFactory";
    /**
     * Connection factory type if using JMS 1.0, either DESTINATION_TYPE_QUEUE or DESTINATION_TYPE_TOPIC
     */
    public static final String PARAM_CONFAC_TYPE = "transport.jms.ConnectionFactoryType";
    /**
     * The Parameter name indicating the JMS connection factory JNDI name
     */
    public static final String PARAM_CONFAC_JNDI_NAME = "transport.jms.ConnectionFactoryJNDIName";
	/**
	 * Duration in milliseconds to try reconnecting when the MQ is
	 * down(shutdown)
	 */
	public static final String PARAM_RECONNECT_INTERVAL = "transport.jms.ReconnectInterval";
    /**
     * The Parameter indicating the expected content type for messages received by the service.
     */
    public static final String CONTENT_TYPE_PARAM = "transport.jms.ContentType";
    /**
     * The Parameter indicating a final EPR as a String, to be published on the WSDL of a service
     * Could occur more than once, and could provide additional connection properties or a subset
     * of the properties auto computed. Also could replace IP addresses with hostnames, and expose
     * public credentials clients. If a user specified this parameter, the auto generated EPR will
     * not be exposed - unless an instance of this parameter is added with the string "legacy"
     * This parameter could be used to expose EPR's conforming to the proposed SOAP/JMS spec
     * until such time full support is implemented for it.
     */
    public static final String PARAM_PUBLISH_EPR = "transport.jms.PublishEPR";
    /** The parameter indicating the JMS API specification to be used - if this is "1.1" the JMS
     * 1.1 API would be used, else the JMS 1.0.2B
     */
    public static final String PARAM_JMS_SPEC_VER = "transport.jms.JMSSpecVersion";

    /**
     * The Parameter indicating whether the JMS Session should be transacted for the service
     * Specified as a "true" or "false"
     */
    public static final String PARAM_SESSION_TRANSACTED = "transport.jms.SessionTransacted";
    /**
     * The Parameter indicating the Session acknowledgement for the service. Must be one of the
     * following Strings, or the appropriate Integer used by the JMS API
     * "AUTO_ACKNOWLEDGE", "CLIENT_ACKNOWLEDGE", "DUPS_OK_ACKNOWLEDGE" or "SESSION_TRANSACTED"
     */
    public static final String PARAM_SESSION_ACK = "transport.jms.SessionAcknowledgement";
    /** A message selector to be used when messages are sought for this service */
    public static final String PARAM_MSG_SELECTOR = "transport.jms.MessageSelector";
    /** Is the Subscription durable ? - "true" or "false" See {@link PARAM_DURABLE_SUB_NAME} */
    public static final String PARAM_SUB_DURABLE = "transport.jms.SubscriptionDurable";
    /** The name for the durable subscription See {@link PARAM_SUB_DURABLE}*/
    public static final String PARAM_DURABLE_SUB_NAME = "transport.jms.DurableSubscriberName";
    /**
     * JMS Resource cachable level to be used for the service One of the following:
     * {@link CACHE_NONE}, {@link CACHE_CONNECTION}, {@link CACHE_SESSION}, {@link CACHE_PRODUCER},
     * {@link CACHE_CONSUMER}, or {@link CACHE_AUTO} - to let the transport decide
     */
    public static final String PARAM_CACHE_LEVEL = "transport.jms.CacheLevel";
    /** Should a pub-sub connection receive messages published by itself? */
    public static final String PARAM_PUBSUB_NO_LOCAL = "transport.jms.PubSubNoLocal";
    /**
     * The number of milliseconds to wait for a message on a consumer.receive() call
     * negative number - wait forever
     * 0 - do not wait at all
     * positive number - indicates the number of milliseconds to wait
     */
    public static final String PARAM_RCV_TIMEOUT = "transport.jms.ReceiveTimeout";
    /**
     * Delay on consume errors.
     */
    public static final String PARAM_CONSUME_ERROR_DELAY = "transport.jms.ConsumeErrorDelay";
    /**
     * Progression factor on consume errors.
     */
    public static final String PARAM_CONSUME_ERROR_PROGRESSION = "transport.jms.ConsumeErrorProgression";
    /**
     * Maximum retries on consume error before delay kicks in.
     */
    public static final String PARAM_MAX_CONSUME_RETRY_BEFORE_DELAY = "transport.jms.MaxConsumeErrorRetriesBeforeDelay";
    /**
     * Maximum retries on consumer error.
     */
    public static final String PARAM_MAX_CONSUME_RETRY_COUNT = "transport.jms.MaxConsumeErrorRetryCount";
    /**
     *The number of concurrent consumers to be created to poll for messages for this service
     * For Topics, this should be ONE, to prevent receipt of multiple copies of the same message
     */
    public static final String PARAM_CONCURRENT_CONSUMERS = "transport.jms.ConcurrentConsumers";
    /**
     * The maximum number of concurrent consumers for the service - See {@link PARAM_CONCURRENT_CONSUMERS}
     */
    public static final String PARAM_MAX_CONSUMERS = "transport.jms.MaxConcurrentConsumers";
    /**
     * The number of idle (i.e. message-less) polling attempts before a worker task commits suicide,
     * to scale down resources, as load decreases
     */
    public static final String PARAM_IDLE_TASK_LIMIT = "transport.jms.IdleTaskLimit";
    /**
     * The maximum number of messages a polling worker task should process, before suicide - to
     * prevent many longer running threads - default is unlimited (i.e. a worker task will live forever)
     */
    public static final String PARAM_MAX_MSGS_PER_TASK = "transport.jms.MaxMessagesPerTask";
    /**
     * Number of milliseconds before the first reconnection attempt is tried, on detection of an
     * error. Subsequent retries follow a geometric series, where the
     * duration = previous duration * factor
     * This is further limited by the {@link PARAM_RECON_MAX_DURATION} to be meaningful
     */
    public static final String PARAM_RECON_INIT_DURATION = "transport.jms.InitialReconnectDuration";
    /** @see PARAM_RECON_INIT_DURATION */
    public static final String PARAM_RECON_FACTOR = "transport.jms.ReconnectProgressFactor";
    /** @see PARAM_RECON_INIT_DURATION */
    public static final String PARAM_RECON_MAX_DURATION = "transport.jms.MaxReconnectDuration";

    /** The username to use when obtaining a JMS Connection */
    public static final String PARAM_JMS_USERNAME = "transport.jms.UserName";
    /** The password to use when obtaining a JMS Connection */
    public static final String PARAM_JMS_PASSWORD = "transport.jms.Password";

     public static final String PARAM_DURABLE_SUB_CLIENT_ID =
            "transport.jms.DurableSubscriberClientID";

    //-------------- message context / transport header properties and client options --------------
    /**
     * A MessageContext property or client Option indicating the JMS message type
     */
    public static final String JMS_MESSAGE_TYPE = "JMS_MESSAGE_TYPE";
    /**
     * The message type indicating a BytesMessage. See {@link JMS_MESSAGE_TYPE}
     */
    public static final String JMS_BYTE_MESSAGE = "JMS_BYTE_MESSAGE";
    /**
     * The message type indicating a TextMessage. See {@link JMS_MESSAGE_TYPE}
     */
    public static final String JMS_TEXT_MESSAGE = "JMS_TEXT_MESSAGE";
    /**
     * The message type indicating a MapMessage. See {@link JMS_MESSAGE_TYPE}
     */
    public static final String JMS_MAP_MESSAGE = "JMS_MAP_MESSAGE";
    /**
     * A MessageContext property or client Option indicating the time to wait for a response JMS message
     */
    public static final String JMS_WAIT_REPLY = "JMS_WAIT_REPLY";
    /**
     * A MessageContext property or client Option indicating the JMS correlation id
     */
    public static final String JMS_COORELATION_ID = "JMS_COORELATION_ID";
    /**
     * A MessageContext property or client Option indicating the JMS message id
     */
    public static final String JMS_MESSAGE_ID = "JMS_MESSAGE_ID";
    /**
     * A MessageContext property or client Option indicating the JMS delivery mode as an Integer or String
     * Value 1 - javax.jms.DeliveryMode.NON_PERSISTENT
     * Value 2 - javax.jms.DeliveryMode.PERSISTENT
     */
    public static final String JMS_DELIVERY_MODE = "JMS_DELIVERY_MODE";

    /**
     * Acceptable String values (PERSISTENT and NON_PERSISTENT) for JMS_DELIVERY_MODE in axis2 scope
     */
    public static final String JMS_PERSISTENT_DELIVERY_MODE = "PERSISTENT";
    public static final String JMS_NON_PERSISTENT_DELIVERY_MODE = "NON_PERSISTENT";


    /**
     * A MessageContext property or client Option indicating the JMS destination to use on a Send
     */
    public static final String JMS_DESTINATION = "JMS_DESTINATION";
    /**
     * A MessageContext property or client Option indicating the JMS message expiration - a Long value
     * specified as a String
     */
    public static final String JMS_EXPIRATION = "JMS_EXPIRATION";
    /**
     * A MessageContext property indicating if the message is a redelivery (Boolean as a String)
     */
    public static final String JMS_REDELIVERED = "JMS_REDELIVERED";
    /**
     * A MessageContext property or client Option indicating the JMS replyTo Destination
     */
    public static final String JMS_REPLY_TO = "JMS_REPLY_TO";
    /**
     * A MessageContext property or client Option indicating the JMS replyTo Destination type
     * See {@link DESTINATION_TYPE_QUEUE} and {@link DESTINATION_TYPE_TOPIC}
     */
    public static final String JMS_REPLY_TO_TYPE = "JMS_REPLY_TO_TYPE";
    /**
     * A MessageContext property or client Option indicating the JMS timestamp (Long specified as String)
     */
    public static final String JMS_TIMESTAMP = "JMS_TIMESTAMP";
    /**
     * A MessageContext property indicating the JMS type String returned by {@link javax.jms.Message.getJMSType()}
     */
    public static final String JMS_TYPE = "JMS_TYPE";
    /**
     * A MessageContext property or client Option indicating the JMS priority
     */
    public static final String JMS_PRIORITY = "JMS_PRIORITY";
    /**
     * A MessageContext property or client Option indicating the JMS time to live for message sent
     */
    public static final String JMS_TIME_TO_LIVE = "JMS_TIME_TO_LIVE";

    /** The prefix that denotes JMSX properties */
    public static final String JMSX_PREFIX = "JMSX";
    /** The JMSXGroupID property */
    public static final String JMSX_GROUP_ID = "JMSXGroupID";
    /** The JMSXGroupSeq property */
    public static final String JMSX_GROUP_SEQ = "JMSXGroupSeq";

    /**Namespace for JMS map payload representation*/
    public static final String JMS_MAP_NS = "http://axis.apache.org/axis2/java/transports/jms/map-payload";

    /**Root element name of JMS Map message payload representation */
    public static final String JMS_MAP_ELEMENT_NAME = "JMSMap";

    public static final QName JMS_MAP_QNAME = new QName(JMS_MAP_NS, JMS_MAP_ELEMENT_NAME,"");

    /**
      * Naming factory initial
      */
     public static final String NAMING_FACTORY_INITIAL = "java.naming.factory.initial";
     /**
      * Default Connection Factory
      * */
     public static final String CONNECTION_STRING = "connectionfactory.QueueConnectionFactory";
     /**
      * JNDI Topic prefix
      * */
     public static final String TOPIC_PREFIX = "topic.";
     /**
      * JNDI Queue prefix
      * */
     public static final String QUEUE_PREFIX = "queue.";

    /**
     *  The content type of a receiving payload
     */
     public static final String CONTENT_TYPE = "ContentType";

     /**
      * Andes Naming Factory
      * */
     public static final String ANDES_NAMING_FACTORY = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";

      /**
     * Constant that holds the name of the environment property
     * for specifying configuration information for the service provider
     * to use. The value of the property should contain a URL string
     * (e.g. "ldap://somehost:389").
     * This property may be specified in the environment,
     * an applet parameter, a system property, or a resource file.
     * If it is not specified in any of these sources,
     * the default configuration is determined by the service provider.
     *
     * <p> The value of this constant is "java.naming.provider.url".
     */
     public static final String PROVIDER_URL = "java.naming.provider.url";

    /**
      * Maximum number of shared JMS Connections when sending messages out
      * */
     public static final String MAX_JMS_CONNECTIONS_ = "transport.jms.MaxJMSConnections";

    /**
     * Does the JMS broker support hyphen in JMS message property names.
     */
    public static final String PARAM_JMS_HYPHEN_MODE = "transport.jms.MessagePropertyHyphens";

    public static final String HYPHEN_MODE_NONE = "none";

    public static final String HYPHEN_MODE_REPLACE = "replace";

    public static final String HYPHEN_MODE_DELETE = "delete";

    public static final String HYPHEN_REPLACEMENT_STR = "_DASHED_";

    public static final String DEFAULT_HYPHEN_SUPPORT = HYPHEN_MODE_NONE;

    /**
     * JMS 2.0 feature: Message delay for the given time interval
     */
    public static final String JMS_MESSAGE_DELAY = "JMS_MESSAGE_DELAY";

    public static final String JMS_SPEC_VERSION = "transport.jms.JMSSpecVersion";

    public static final String JMS_XA_TRANSACTION_PREFIX = "wso2-jms-xa-transaction";

    public static final String JMS_XA_TRANSACTION = "distributedTx";

    public static final String JMS_XA_TRANSACTION_MANAGER = "distributedTxMgr";

    /**
    * Will be used identify transactionality.
    */
    public static final String JMS_TRANSACTION_COMMAND = "transport.jms.TransactionCommand";

    public static final String JMS_SPEC_VERSION_1_1 = "1.1";

    public static final String JMS_SPEC_VERSION_2_0 = "2.0";

    public static final String JMS_SPEC_VERSION_1_0 = "1.0.2b";

    public static final String PARAM_IS_SHARED_SUBSCRIPTION = "transport.jms.SharedSubscription";

    public static final String DELIVERY_COUNT = "jms.message.delivery.count";

    public static final String JMS_MESSAGE_DELIVERY_COUNT_HEADER = "JMSXDeliveryCount";

    /**
     * Message context level property indicating if JMS session need to be recovered associated with the message
     */
    public static final String SET_RECOVER = "SET_RECOVER";

    /**
     * Parameter for jndi security credentials in jms configs of axis2.xml
     */
    public static final String PARAM_NAMING_SECURITY_CREDENTIALS = "java.naming.security.credentials";

    /**
     * Qualified name for SecureVault aliases.
     */
    public static final QName ALIAS_QNAME = new QName("http://org.wso2.securevault/configuration", "secretAlias");

    /** Is Throttling enabled or not*/
    public static final String JMS_PROXY_THROTTLE_ENABLED = "jms.proxy.throttle.enabled";
    /** Throttling mode specified */
    public static final String JMS_PROXY_THROTTLE_MODE = "jms.proxy.throttle.mode";
    /** Throttling limit of messages per minute */
    public static final String JMS_PROXY_THROTTLE_PER_MIN = "jms.proxy.throttle.limitPerMinute";
    /** Time unit to be used for throttling i.e: minute, day, hour */
    public static final String JMS_PROXY_THROTTLE_TIME_UNIT = "jms.proxy.throttle.timeUnit";
    /** Number of messages to trigger throttling in a unit time unit */
    public static final String JMS_PROXY_THROTTLE_COUNT = "jms.proxy.throttle.count";
    /** Is throttle count read dynamically. True leads to low performance */
    public static final String JMS_PROXY_DYNAMIC_THROTTLE_ENABLED = "jms.proxy.throttle.dynamic.enabled";
    /** System property to be set if the dynamic throttling is enabled */
    public static final String JMS_PROXY_DYNAMIC_THROTTLE_PROPERTY = "jms.proxy.throttle.count.systemProperty";

    public static final String JMS_PROXY_THROTTLE_HOUR = "hour";

    public static final String JMS_PROXY_THROTTLE_MINUTE = "minute";

    public static final String JMS_PROXY_THROTTLE_DAY = "day";

    /** Batch Throttling constant */
    public static final String JMS_PROXY_BATCH_THROTTLE = "batch";
    /** Fixed Interval Throttlinh constant */
    public static final String JMS_PROXY_FIXED_INTERVAL_THROTTLE = "fixed-interval";

    // Error handling properties.
    static final String SENDING_FAULT = "SENDING_FAULT";
    static final String ERROR_MESSAGE = "ERROR_MESSAGE";
}

/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.axis2.transport.mqtt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * This class handles asynchronous call
 */

public class MqttAsyncCallback implements MqttCallback {

    int state = BEGIN;

    public static final int BEGIN = 0;
    public static final int CONNECTED = 1;
    public static final int PUBLISHED = 2;
    public static final int SUBSCRIBED = 3;
    public static final int DISCONNECTED = 4;
    public static final int FINISH = 5;
    public static final int ERROR = 6;
    public static final int DISCONNECT = 7;

    private MqttConnectOptions conOpt;
    private Log log = LogFactory.getLog(MqttAsyncCallback.class);

    // Private instance variables
    private MqttAsyncClient client;
    private String brokerUrl;
    private Throwable ex = null;
    private final Object waiter = new Object();
    private boolean donext = false;

    public void setConOpt(MqttConnectOptions conOpt) {
        this.conOpt = conOpt;
    }

    public MqttAsyncCallback(MqttAsyncClient clientAsync) throws MqttException {
        client = clientAsync;
        // Set this wrapper as the callback handler.
        client.setCallback(this);
    }

    /**
     * Publish / send a message to an MQTT server.
     *
     * @param topicName the name of the topic to publish to
     * @param message   the set of bytes to send to the MQTT server
     * @throws MqttException
     */
    public void publish(String topicName, MqttMessage message) throws Throwable {
         /*
         Use a state machine to decide which step to do next. State change occurs
         when a notification is received that an MQTT action has completed
         */
        while (state != FINISH) {
            switch (state) {
                case BEGIN:
                    // Connect using a non-blocking connect
                    MqttConnector con = new MqttConnector();
                    con.doConnect();
                    break;
                case CONNECTED:
                    // Publish using a non-blocking publisher
                    Publisher pub = new Publisher();
                    pub.doPublish(topicName, message);
                    break;
                case PUBLISHED:
                    state = DISCONNECT;
                    donext = true;
                    break;
                case DISCONNECT:
                    Disconnector disc = new Disconnector();
                    disc.doDisconnect();
                    break;
                case ERROR:
                    throw ex;
                case DISCONNECTED:
                    state = FINISH;
                    donext = true;
                    break;
            }
            waitForStateChange(MqttConstants.WAIT_TIME);
        }
    }

    /**
     * Wait for a maximum amount of time for a state change event to occur.
     *
     * @param maxTTW maximum time to wait in milliseconds
     * @throws MqttException
     */
    private void waitForStateChange(int maxTTW) throws MqttException {
        synchronized (waiter) {
            if (!donext) {
                try {
                    waiter.wait(maxTTW);
                } catch (InterruptedException e) {
                    log.error("Error while waiting for state change",e);
                }
                if (ex != null) {
                    throw (MqttException) ex;
                }
            }
            donext = false;
        }
    }

    /**
     * Subscribe to a topic on an MQTT server.
     * Once subscribed this method waits for the messages to arrive from the server
     * that match the subscription. It continues listening for messages until the enter key is
     * pressed.
     *
     * @param topicName to subscribe to (can be wild carded)
     * @param qos       the maximum quality of service to receive messages at for this subscription
     * @throws MqttException
     */
    public void subscribe(String topicName, int qos) throws Throwable {
        /**
         Use a state machine to decide which step to do next. State change occurs
         when a notification is received that an MQTT action has completed.
         */
        while (state != FINISH) {
            switch (state) {
                case BEGIN:
                    // Connect using a non-blocking connect.
                    MqttConnector con = new MqttConnector();
                    con.doConnect();
                    break;
                case CONNECTED:
                    // Subscribe using a non-blocking subscribe
                    Subscriber sub = new Subscriber();
                    sub.doSubscribe(topicName, qos);
                    break;
                case SUBSCRIBED:
                    // Block until Enter is pressed allowing messages to arrive.
                    log.info("Press <Enter> to exit");
                    try {
                        System.in.read();
                    } catch (IOException e) {
                        //If we can't read we'll just exit.
                    }
                    state = DISCONNECT;
                    donext = true;
                    break;
                case DISCONNECT:
                    Disconnector disc = new Disconnector();
                    disc.doDisconnect();
                    break;
                case ERROR:
                    throw ex;
                case DISCONNECTED:
                    state = FINISH;
                    donext = true;
                    break;
            }
            waitForStateChange(MqttConstants.WAIT_TIME);
        }
    }


    public void connectionLost(Throwable throwable) {
        /*
        Implements from MqttCallback
         */
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        throw new IllegalStateException();
    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

        log.info("message delivered .. : " + iMqttDeliveryToken.toString());
    }

    /**
     * Connect in a non-blocking way and then sit back and wait to be
     * notified that the action has completed.
     */
    public class MqttConnector {

        public MqttConnector() {
        }

        public void doConnect() {
            /**
             Connect to the server
             Get a token and setup an asynchronous listener on the token which
             will be notified once the connect completes
             */
            if (log.isDebugEnabled()){
                log.debug("Connecting to " + brokerUrl + " with client ID " + client.getClientId());
            }
            IMqttActionListener conListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    if (log.isDebugEnabled()){
                        log.debug("Connected");
                    }
                    state = CONNECTED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    if(log.isDebugEnabled()){
                        log.debug("connect failed" + exception);
                    }
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        donext = true;
                        waiter.notifyAll();
                    }
                }
            };

            try {
                // Connect using a non-blocking connect
                client.connect(conOpt, "Connect sample context", conListener);

            } catch (MqttException e) {
                /**
                 If though it is a non-blocking connect an exception can be
                 thrown if validation of parms fails or other checks such
                 as already connected fail.
                 */
                state = ERROR;
                donext = true;
                ex = e;
            }
        }
    }

    /**
     * Publish in a non-blocking way and then sit back and wait to be
     * notified that the action has completed.
     */
    public class Publisher {
        public void doPublish(String topicName, MqttMessage message) {
            /**
             Send / publish a message to the server.
             Get a token and setup an asynchronous listener on the token which
             will be notified once the message has been delivered.
             */

            String time = new Timestamp(System.currentTimeMillis()).toString();

            // Setup a listener object to be notified when the publish completes.
            IMqttActionListener pubListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    if (log.isDebugEnabled()){
                        log.debug("Publish Completed");
                    }
                    state = PUBLISHED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    if (log.isDebugEnabled()){
                        log.debug("Publish failed" + exception);
                    }
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        donext = true;
                        waiter.notifyAll();
                    }
                }
            };
            try {
                // Publish the message.
                client.publish(topicName, message, "Pub sample context", pubListener);
            } catch (MqttException e) {
                state = ERROR;
                donext = true;
                ex = e;
            }
        }
    }

    /**
     * Subscribe in a non-blocking way and then sit back and wait to be
     * notified that the action has completed.
     */
    public class Subscriber {
        public void doSubscribe(final String topicName, int qos) {
            /*
            Make a subscription.
            Get a token and setup an asynchronous listener on the token which
            will be notified once the subscription is in place.
            */
            if (log.isDebugEnabled()){
                log.debug("Subscribing to topic \"" + topicName + "\" qos " + qos);
            }
            IMqttActionListener subListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    if (log.isDebugEnabled()){
                        log.debug("Subscribe Completed with the topic"+topicName);
                    }
                    state = SUBSCRIBED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    if (log.isDebugEnabled()){
                        log.debug("Subscribe failed" + exception);
                    }
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        donext = true;
                        waiter.notifyAll();
                    }
                }
            };
            try {
                client.subscribe(topicName, qos, "Subscribe sample context", subListener);
            } catch (MqttException e) {
                state = ERROR;
                donext = true;
                ex = e;
            }
        }
    }

    /**
     * Disconnect in a non-blocking way and then sit back and wait to be
     * notified that the action has completed.
     */
    public class Disconnector {
        public void doDisconnect() {
            // Disconnect the client
            if (log.isDebugEnabled()){
                log.debug("Disconnecting");
            }
            IMqttActionListener discListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    if (log.isDebugEnabled()){
                        log.debug("Disconnect Completed");
                    }
                    state = DISCONNECTED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    if (log.isDebugEnabled()){
                        log.debug("Disconnect failed" + exception);
                    }
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        donext = true;
                        waiter.notifyAll();
                    }
                }
            };
            try {
                client.disconnect("Disconnect sample context", discListener);
            } catch (MqttException e) {
                state = ERROR;
                donext = true;
                ex = e;
            }
        }
    }
}

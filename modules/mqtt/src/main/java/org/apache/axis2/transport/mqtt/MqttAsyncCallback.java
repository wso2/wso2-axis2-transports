package org.apache.axis2.transport.mqtt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.sql.Timestamp;

public class MqttAsyncCallback implements MqttCallback {

    int state = BEGIN;

    static final int BEGIN = 0;
    static final int CONNECTED = 1;
    static final int PUBLISHED = 2;
    static final int SUBSCRIBED = 3;
    static final int DISCONNECTED = 4;
    static final int FINISH = 5;
    static final int ERROR = 6;
    static final int DISCONNECT = 7;


    private MqttConnectOptions 	conOpt;
    private Log log = LogFactory.getLog(MqttAsyncCallback.class);


    // Private instance variables
    private MqttAsyncClient client;
    private String 				brokerUrl;
    private Throwable 			ex = null;
    private final Object 				waiter = new Object();
    private boolean 			donext = false;


    public void setConOpt(MqttConnectOptions conOpt) {
        this.conOpt = conOpt;
    }



    public MqttAsyncCallback(MqttAsyncClient clientAsync) throws MqttException {

            client = clientAsync;
            // Set this wrapper as the callback handler
            client.setCallback(this);

    }

    /**
     * Publish / send a message to an MQTT server
     * @param topicName the name of the topic to publish to
     * @param message the set of bytes to send to the MQTT server
     * @throws MqttException
     */
    public void publish(String topicName, MqttMessage message) throws Throwable {
        // Use a state machine to decide which step to do next. State change occurs
        // when a notification is received that an MQTT action has completed
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

//    		if (state != FINISH) {
            // Wait until notified about a state change and then perform next action
            waitForStateChange(10000);
//    		}
        }
    }

    /**
     * Wait for a maximum amount of time for a state change event to occur
     * @param maxTTW  maximum time to wait in milliseconds
     * @throws MqttException
     */
    private void waitForStateChange(int maxTTW ) throws MqttException {
        synchronized (waiter) {
            if (!donext ) {
                try {
                    waiter.wait(maxTTW);
                } catch (InterruptedException e) {
                    log.info("timed out");
                    e.printStackTrace();
                }

                if (ex != null) {
                    throw (MqttException)ex;
                }
            }
            donext = false;
        }
    }

    /**
     * Subscribe to a topic on an MQTT server
     * Once subscribed this method waits for the messages to arrive from the server
     * that match the subscription. It continues listening for messages until the enter key is
     * pressed.
     * @param topicName to subscribe to (can be wild carded)
     * @param qos the maximum quality of service to receive messages at for this subscription
     * @throws MqttException
     */
    public void subscribe(String topicName, int qos) throws Throwable {
        // Use a state machine to decide which step to do next. State change occurs
        // when a notification is received that an MQTT action has completed
        while (state != FINISH) {
            switch (state) {
                case BEGIN:
                    // Connect using a non-blocking connect
                    MqttConnector con = new MqttConnector();
                    con.doConnect();
                    break;
                case CONNECTED:
                    // Subscribe using a non-blocking subscribe
                    Subscriber sub = new Subscriber();
                    sub.doSubscribe(topicName, qos);
                    break;
                case SUBSCRIBED:
                    // Block until Enter is pressed allowing messages to arrive
                    log.info("Press <Enter> to exit");
                    try {
                        System.in.read();
                    } catch (IOException e) {
                        //If we can't read we'll just exit
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

//    		if (state != FINISH && state != DISCONNECT) {
            waitForStateChange(10000);
        }
//    	}
    }


    public void connectionLost(Throwable throwable) {
        //ignoring for the moment...
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
            // Connect to the server
            // Get a token and setup an asynchronous listener on the token which
            // will be notified once the connect completes
            log.info("Connecting to " + brokerUrl + " with client ID " + client.getClientId());

            IMqttActionListener conListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.info("Connected");
                    state = CONNECTED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    log.info("connect failed" + exception);
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        donext=true;
                        waiter.notifyAll();
                    }
                }
            };

            try {
                // Connect using a non-blocking connect
                client.connect(conOpt,"Connect sample context", conListener);

            } catch (MqttException e) {
                // If though it is a non-blocking connect an exception can be
                // thrown if validation of parms fails or other checks such
                // as already connected fail.
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
            // Send / publish a message to the server
            // Get a token and setup an asynchronous listener on the token which
            // will be notified once the message has been delivered
           // MqttMessage message = new MqttMessage(payload);
            //message.setQos(qos);


            String time = new Timestamp(System.currentTimeMillis()).toString();
          //  log.info("Publishing at: "+time+ " to topic \""+topicName+"\" qos "+qos);

            // Setup a listener object to be notified when the publish completes.
            //
            IMqttActionListener pubListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.info("Publish Completed");
                    state = PUBLISHED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    log.info("Publish failed" + exception);
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        donext=true;
                        waiter.notifyAll();
                    }
                }
            };

            try {
                // Publish the message
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
        public void doSubscribe(String topicName, int qos) {
            // Make a subscription
            // Get a token and setup an asynchronous listener on the token which
            // will be notified once the subscription is in place.
            log.info("Subscribing to topic \"" + topicName + "\" qos " + qos);

            IMqttActionListener subListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.info("Subscribe Completed");
                    state = SUBSCRIBED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    log.info("Subscribe failed" + exception);
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        donext=true;
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
            log.info("Disconnecting");

            IMqttActionListener discListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.info("Disconnect Completed");
                    state = DISCONNECTED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    log.info("Disconnect failed" + exception);
                    carryOn();
                }
                public void carryOn() {
                    synchronized (waiter) {
                        donext=true;
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

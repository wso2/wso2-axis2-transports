package test;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class SampleMqttCallBack implements MqttCallback {
    public static String message;

    public void connectionLost(Throwable throwable) {
        System.out.println("Connection lost...");
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

        System.out.println("Message arrived...");
        System.out.println("Topic : " + s);
        System.out.println("Message : " + mqttMessage.toString());
        this.message=mqttMessage.toString();
        boolean isMessage=message.contains("WSO2");
        assert (isMessage);
    }
    public void a(){
    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("Delivery complete....");
        System.out.println("Delivery Token : " + iMqttDeliveryToken.toString());
    }
}

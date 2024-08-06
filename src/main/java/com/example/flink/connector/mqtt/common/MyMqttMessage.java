package com.example.flink.connector.mqtt.common;


import java.io.Serializable;

public class MyMqttMessage implements Serializable {

    private static final long serialVersionUID = -4673414704450588069L;

    private String topic;
    private String payload;

    public MyMqttMessage() {
    }

    public MyMqttMessage(String topic, String payload) {
        super();
        this.topic = topic;
        this.payload = payload;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}

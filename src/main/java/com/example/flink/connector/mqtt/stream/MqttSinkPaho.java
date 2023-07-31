package com.example.flink.connector.mqtt.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSinkPaho<T> extends RichSinkFunction<T> {
    private static final Logger log = LoggerFactory.getLogger(MqttSinkPaho.class);
    private static final long serialVersionUID = 7883296716671354462L;

    private transient MqttClient client;
    private String topic;
    private String broker;
    private String username;
    private String password;

    public MqttSinkPaho(String broker, String username, String password, String topic) {
        this.broker = broker;
        this.username = username;
        this.password = password;
        this.topic = topic;
    }

    @Override
    public void invoke(T event, Context context) throws Exception {
        log.info("MqttSink invoke...");
        byte[] payload = event.toString().getBytes();
        log.info("messge is {}", event);
        int qos = 0;
        // 创建消息并设置 QoS
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);
        client.publish(topic, message);
    }

    @Override
    public void close() throws Exception {
        log.info("MqttSink close...");
        super.close();
        // 关闭连接
        client.disconnect();
        // 关闭客户端
        client.close();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("MqttSink open...");
        super.open(parameters);
        String clientId = "FlinkTest_" + Thread.currentThread().getId() % 10000 + "_" + System.currentTimeMillis() % 100000;
        client = new MqttClient(broker, clientId, new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        //自动重新连接，默认为false
        options.setAutomaticReconnect(true);
        client.connect(options);
    }


}

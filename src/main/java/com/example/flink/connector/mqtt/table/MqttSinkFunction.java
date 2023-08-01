package com.example.flink.connector.mqtt.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class MqttSinkFunction<T> extends RichSinkFunction<T> {
    private static final Logger log = LoggerFactory.getLogger(MqttSinkFunction.class);

    private static final long serialVersionUID = -6429278620184672870L;
    private transient MqttClient client;
    //MQTT连接配置信息
    private final String topics;
    private final String hostUrl;
    private final String username;
    private final String password;
    private final Integer qos;
    private final String clientIdPrefix;
    private final Integer connectionTimeout;
    private final Integer keepAliveInterval;
    private final boolean automaticReconnect;
    private final SerializationSchema<T> serializer;

    public MqttSinkFunction(String hostUrl, String username, String password, String topics, Integer qos, String clientIdPrefix, Integer connectionTimeout, Integer keepAliveInterval, boolean automaticReconnect, SerializationSchema<T> serializer) {
        this.hostUrl = hostUrl;
        this.username = username;
        this.password = password;
        this.topics = topics;
        this.qos = qos;
        this.clientIdPrefix = clientIdPrefix;
        this.connectionTimeout = connectionTimeout;
        this.keepAliveInterval = keepAliveInterval;
        this.automaticReconnect = automaticReconnect;
        this.serializer = serializer;
    }


    @Override
    public void invoke(T event, Context context) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("sink invoke...");
            log.debug("message is {}", event);
        }
        byte[] payload = this.serializer.serialize(event);
        // 创建消息并设置 QoS
        MqttMessage message = new MqttMessage(payload);
        message.setQos(this.qos);
        String[] topics = this.topics.split(",");
        for (String topic : topics) {
            this.client.publish(topic, message);
        }
    }

    @Override
    public void close() throws Exception {
        log.info("sink close...");
        super.close();
        // 关闭连接
        if (this.client.isConnected()) {
            this.client.disconnect();
            // 关闭客户端
            this.client.close();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("sink open...");
        super.open(parameters);
        String clientId = this.clientIdPrefix + "_" + UUID.randomUUID();
        this.client = new MqttClient(this.hostUrl, clientId, new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(this.username);
        options.setPassword(this.password.toCharArray());
        // 设置超时时间
        options.setConnectionTimeout(this.connectionTimeout);
        // 设置会话心跳时间
        options.setKeepAliveInterval(this.keepAliveInterval);
        //自动重新连接，默认为false
        options.setAutomaticReconnect(this.automaticReconnect);
        this.client.connect(options);
    }


}

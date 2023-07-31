package com.example.flink.connector.mqtt.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.flink.connector.mqtt.table.MqttDynamicTableFactory.*;

public class MqttSinkFunction<T> extends RichSinkFunction<T> {
    private static final Logger log = LoggerFactory.getLogger(MqttSinkFunction.class);

    private static final long serialVersionUID = -6429278620184672870L;
    private transient MqttClient client;
    //MQTT连接配置信息
    private ReadableConfig conf;
    private SerializationSchema<T> serializer;

    public MqttSinkFunction(ReadableConfig conf, SerializationSchema<T> serializer) {
        this.conf = conf;
        this.serializer = serializer;
    }


    @Override
    public void invoke(T event, Context context) throws Exception {
        log.info("sink invoke...");
        log.info("message is {}", event);
        byte[] payload = this.serializer.serialize(event);
        int qos = 0;
        // 创建消息并设置 QoS
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);

        this.client.publish(this.conf.get(TOPIC), message);
    }

    @Override
    public void close() throws Exception {
        log.info("sink close...");
        super.close();
        // 关闭连接
        if (this.client.isConnected()) {
            client.disconnect();
            // 关闭客户端
            client.close();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("sink open...");
        super.open(parameters);
        this.client = new MqttClient(this.conf.get(HOSTURL), conf.get(CLIENTID), new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(this.conf.get(USERNAME));
        options.setPassword(this.conf.get(PASSWORD).toCharArray());
        //自动重新连接，默认为false
        options.setAutomaticReconnect(true);
        this.client.connect(options);
    }


}

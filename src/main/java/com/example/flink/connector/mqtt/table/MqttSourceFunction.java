package com.example.flink.connector.mqtt.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class MqttSourceFunction<T> extends RichSourceFunction<T> {
    private static final Logger log = LoggerFactory.getLogger(MqttSourceFunction.class);

    private static final long serialVersionUID = 6241249297629516864L;
    private transient volatile boolean running;
    //阻塞队列存储订阅的消息
    private final BlockingQueue<T> queue = new ArrayBlockingQueue<>(10);
    //存储服务
    private MqttClient client;
    //MQTT连接配置信息
    private final String topics;
    private final String hostUrl;
    private final String username;
    private final String password;
    private final String clientIdPrefix;
    private final boolean automaticReconnect;
    private final boolean cleanSession;
    private final Integer connectionTimeout;
    private final Integer keepAliveInterval;
    //存储订阅主题
    private final DeserializationSchema<T> deserializer;

    public MqttSourceFunction(String hostUrl, String username, String password, String topics, boolean cleanSession, String clientIdPrefix, boolean automaticReconnect, Integer connectionTimeout, Integer keepAliveInterval, DeserializationSchema<T> deserializer) {
        this.topics = topics;
        this.hostUrl = hostUrl;
        this.username = username;
        this.password = password;
        this.cleanSession = cleanSession;
        this.clientIdPrefix = clientIdPrefix;
        this.automaticReconnect = automaticReconnect;
        this.connectionTimeout = connectionTimeout;
        this.keepAliveInterval = keepAliveInterval;
        this.deserializer = deserializer;
    }

    //包装连接的方法
    private void connect() throws MqttException {
        //连接mqtt服务器
        log.info("source connect...");
        String clientId = this.clientIdPrefix + "_" + UUID.randomUUID();
        this.client = new MqttClient(this.hostUrl, clientId, new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(this.username);
        options.setPassword(this.password.toCharArray());
        options.setCleanSession(this.cleanSession);   //是否清除session
        // 设置超时时间
        options.setConnectionTimeout(this.connectionTimeout);
        // 设置会话心跳时间
        options.setKeepAliveInterval(this.keepAliveInterval);
        options.setAutomaticReconnect(this.automaticReconnect);

        String[] topics = this.topics.split(",");
        //订阅消息
        this.client.connect(options);
        this.client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                // 处理连接断开的情况
                log.warn("source 连接丢失...");
                try {
                    reconnect(); // 进行重连
                } catch (MqttException e) {
                    log.error("source 重连失败", e);
                }
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
                if (log.isDebugEnabled()) {
                    log.debug("接收消息主题:" + topic);
                    log.debug("接收消息Qos:" + mqttMessage.getQos());
                    log.debug("接收消息内容:\n" + new String(mqttMessage.getPayload()));
                }
                T deserialize;
                try {
                    deserialize = deserializer.deserialize(mqttMessage.getPayload());
                } catch (Exception e) {
                    log.error("反序列化mqtt消息异常，消息内容【{}】", mqttMessage.getPayload(), e);
                    return;
                }
                try {
                    queue.put(deserialize);
                } catch (Exception e) {
                    log.error("queue收集消息异常，消息【{}】", deserialize, e);
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                // 消息交付完成的回调方法
                if (log.isDebugEnabled()) {
                    log.debug("source deliveryComplete---------" + iMqttDeliveryToken.isComplete());
                }
            }
        });

        this.client.subscribe(topics);
    }

    private void reconnect() throws MqttException {
        log.warn("source mqtt尝试重连");
        if (this.client.isConnected()) {
            this.client.disconnect();
            this.client.close();
        }
        connect();
    }

    //flink线程启动函数
    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        log.info("source run...");
        this.running = true;
        connect();
        //利用死循环使得程序一直监控主题是否有新消息
        while (this.running) {
            //使用阻塞队列的好处是队列空的时候程序会一直阻塞到这里不会浪费CPU资源
            ctx.collect(this.queue.take());
        }
    }

    @Override
    public void cancel() {
        log.info("source cancel...");
        this.running = false;
        try {
            if (null != this.client && this.client.isConnected()) {
                this.client.disconnect();
                this.client.close();
            }
        } catch (MqttException e) {
            log.error("断开连接异常", e);
        }
    }
}

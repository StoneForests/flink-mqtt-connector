package com.example.flink.connector.mqtt.stream;

import com.example.flink.connector.mqtt.common.MyMqttMessage;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class MqttSourcePaho extends RichSourceFunction<MyMqttMessage> implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(MqttSourcePaho.class);
    private static final long serialVersionUID = -7193959421805098938L;
    private final String topic;
    private transient MqttClient client;
    private transient volatile boolean running;
    private final String broker;
    private transient SourceContext<MyMqttMessage> ctx;
    private final String userName;
    private final String password;

    public MqttSourcePaho(String broker, String username, String password, String topic, Integer qos) throws MqttException {
        this.broker = broker;
        this.topic = topic;
        this.userName = username;
        this.password = password;
    }

    private void connect() throws MqttException {
        log.info("source connect...");

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setUserName(this.userName);
        connOpts.setPassword(this.password.toCharArray());
        //自动重新连接，默认为false
        connOpts.setAutomaticReconnect(true);
        //此值默认为true
        // 如果设置为 false，客户端和服务器将在客户端、服务器和连接重新启动时保持状态。随着状态的保持：
        // 即使客户端、服务器或连接重新启动，消息传递也将可靠地满足指定的 QOS。服务器将订阅视为持久的。
        // 如果设置为 true，客户端和服务器将不会在客户端、服务器或连接重新启动时保持状态
        connOpts.setCleanSession(true);
        //该值以秒为单位，必须>0，定义了客户端等待与 MQTT 服务器建立网络连接的最大时间间隔。
        // 默认超时为 30 秒。值 0 禁用超时处理，这意味着客户端将等待直到网络连接成功或失败。
        connOpts.setConnectionTimeout(30);
        //此值以秒为单位，默认为60，定义发送或接收消息之间的最大时间间隔，必须>0
        connOpts.setKeepAliveInterval(60);
        this.client = new MqttClient(broker, MqttClient.generateClientId(), new MemoryPersistence());
        this.client.connect(connOpts);

        this.client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                // 处理连接断开的情况
                try {
                    reconnect(); // 进行重连
                } catch (MqttException e) {
                    // 处理重连异常
                }
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
                if (log.isDebugEnabled()) {
                    log.debug("接收消息主题:" + topic);
                    log.debug("接收消息Qos:" + mqttMessage.getQos());
                    log.debug("接收消息内容:\n" + new String(mqttMessage.getPayload()));
                }
                String payload = new String(mqttMessage.getPayload());
                MyMqttMessage message = new MyMqttMessage(topic, payload);
                if (payload.equals("stop")) {
                    running = false;
                }
                ctx.collect(message); // 将消息发送到 Flink 数据流
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                // 消息交付完成的回调方法
                if (log.isDebugEnabled()) {
                    log.debug("deliveryComplete---------" + iMqttDeliveryToken.isComplete());
                }
            }
        });

        this.client.subscribe(topic);
    }

    private void reconnect() throws MqttException {
        this.client.disconnect();
        this.client.close();
        connect();
    }

    @Override
    public void run(SourceContext<MyMqttMessage> sourceContext) throws MqttException, InterruptedException {
        log.info("source run...");

        this.ctx = sourceContext;
        this.running = true;
        connect();
        while (running) {
            // 继续运行，直到调用 cancel() 方法
            if (log.isDebugEnabled()) {
                log.debug("MqttSourcePaho keep running...");
            }
            Thread.sleep(10000);
        }
    }

    @Override
    public void cancel() {
        log.info("source cancel...");

        running = false;
        // 关闭连接
        try {
            if (null != client && client.isConnected()) {
                client.disconnect();
                client.close();
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}

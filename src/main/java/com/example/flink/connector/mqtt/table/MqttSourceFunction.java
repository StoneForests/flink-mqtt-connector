package com.example.flink.connector.mqtt.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.example.flink.connector.mqtt.table.MqttDynamicTableFactory.*;


public class MqttSourceFunction extends RichSourceFunction<RowData> {
    private static final Logger log = LoggerFactory.getLogger(MqttSourceFunction.class);

    private static final long serialVersionUID = 6241249297629516864L;
    private transient volatile boolean running;
    //MQTT连接配置信息
    private ReadableConfig conf;
    //阻塞队列存储订阅的消息
    private BlockingQueue<RowData> queue = new ArrayBlockingQueue<>(10);
    //存储服务
    private MqttClient client;
    //存储订阅主题
    private DeserializationSchema<RowData> deserializer;

    public MqttSourceFunction(ReadableConfig options, DeserializationSchema<RowData> deserializer) {
        this.conf = options;
        this.deserializer = deserializer;
    }

    //包装连接的方法
    private void connect() throws MqttException {
        //连接mqtt服务器
        log.info("source connect...");
        this.client = new MqttClient(conf.get(HOSTURL), conf.get(CLIENTID), new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(this.conf.get(USERNAME));
        options.setPassword(this.conf.get(PASSWORD).toCharArray());
        options.setCleanSession(true);   //是否清除session
        // 设置超时时间
        options.setConnectionTimeout(30);
        // 设置会话心跳时间
        options.setKeepAliveInterval(60);
        options.setAutomaticReconnect(true);

        String[] topics = this.conf.get(TOPIC).split(",");
        //订阅消息
        int[] qos = new int[topics.length];
        for (int i = 0; i < topics.length; i++) {
            qos[i] = 1;
        }
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
            public void messageArrived(String topic, MqttMessage mqttMessage) throws IOException, InterruptedException {
//                log.info("接收消息主题:" + topic);
//                log.info("接收消息Qos:" + mqttMessage.getQos());
//                log.info("接收消息内容:" + new String(mqttMessage.getPayload()));
                queue.put(deserializer.deserialize(mqttMessage.getPayload()));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                // 消息交付完成的回调方法
                log.info("source deliveryComplete---------" + iMqttDeliveryToken.isComplete());
            }
        });

        this.client.subscribe(topics, qos);
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
    public void run(SourceContext<RowData> ctx) throws Exception {
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
        running = false;
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

package com.example.flink.connector.mqtt.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.UUID;

public class MqttOptions {
    private MqttOptions() {
    }

    //5、定义MQTT Connector需要的各项参数
    public static final ConfigOption<String> HOST_URL =
            ConfigOptions.key("hostUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect host url.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect password.");

    public static final ConfigOption<String> SINK_TOPICS =
            ConfigOptions.key("sinkTopics")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's sink topic.");

    public static final ConfigOption<String> SOURCE_TOPICS =
            ConfigOptions.key("sourceTopics")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's source topics.");

    public static final ConfigOption<String> CLIENT_ID_PREFIX =
            ConfigOptions.key("clientIdPrefix")
                    .stringType()
                    .defaultValue(String.valueOf(UUID.randomUUID()))
                    .withDescription("the mqtt's connect client id's prefix.");

    public static final ConfigOption<Integer> QOS =
            ConfigOptions.key("qos")
                    .intType()
                    .defaultValue(1)
                    .withDescription("the mqtt's sink qos.");

    public static final ConfigOption<Boolean> AUTOMATIC_RECONNECT =
            ConfigOptions.key("autoReconnect")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("the mqtt's connect automatic reconnect.");

    public static final ConfigOption<Boolean> CLEAN_SESSION =
            ConfigOptions.key("cleanSession")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("the mqtt's source clean session.");

    public static final ConfigOption<Integer> CONNECTION_TIMEOUT =
            ConfigOptions.key("connectionTimeout")
                    .intType()
                    .defaultValue(30)
                    .withDescription("the mqtt's connect timeout.");

    public static final ConfigOption<Integer> KEEP_ALIVE_INTERVAL =
            ConfigOptions.key("keepAliveInterval")
                    .intType()
                    .defaultValue(60)
                    .withDescription("the mqtt's connect keep alive interval.");

//    public static final ConfigOption<Integer> SINK_PARALLELISM =
//            ConfigOptions.key("sinkParallelism")
//                    .intType()
//                    .defaultValue(1)
//                    .withDescription("the mqtt's sink parallelism.");
}

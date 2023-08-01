package com.example.flink.connector.mqtt.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.UUID;

public class MqttOptions {
    private MqttOptions() {
    }

    //TODO 5、定义MQTT Connector需要的各项参数
    public static final ConfigOption<String> HOSTURL =
            ConfigOptions.key("hosturl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect hosturl.");

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

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect topic.");

    public static final ConfigOption<String> CLIENTID =
            ConfigOptions.key("clientid")
                    .stringType()
                    .defaultValue(String.valueOf(UUID.randomUUID()))
                    .withDescription("the mqtt's connect clientId.");

}

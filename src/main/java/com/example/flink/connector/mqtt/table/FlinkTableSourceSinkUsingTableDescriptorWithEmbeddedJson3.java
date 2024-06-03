package com.example.flink.connector.mqtt.table;

import org.apache.flink.table.api.*;

public class FlinkTableSourceSinkUsingTableDescriptorWithEmbeddedJson3 {

    public static void main(String[] args) throws Exception {

        /*
        嵌套json如下
        [{"bad_product_number":11399,"device_id":"wireless_calibration_model_data_df58c7c33482fae9","device_state":0,"good_product_number":43036,"groupName":"base","ip":"172.17.1.20","mark":null,"ts":"2023-11-03T17:32:39.000Z","type":"event"}]
        * */

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        Schema sourceSchema = Schema.newBuilder()
                .column("data", "STRING")
                .build();
        TableDescriptor sourceDescriptor = TableDescriptor
                .forConnector("mqtt")
                .schema(sourceSchema)
                .format("json")
                .option("hostUrl", "tcp://10.0.113.61:1883")
                .option("username", "emqxadmin")
                .option("password", "JevDDzb!5Qfh5Fmr")
                .option("topics", "mqtt/source1")
                .option("clientIdPrefix", "source_client")
                .option("cleanSession", "true")
                .option("autoReconnect", "true")
                .option("connectionTimeout", "30")
                .option("keepAliveInterval", "60")
                .build();
        tEnv.createTemporaryTable("src", sourceDescriptor);

        //查询flink_mqtt_source中的数据，下述语句会阻塞住，
//        TableResult tableResult = tEnv.executeSql("SELECT `B`.`device_id`, `B`.`groupName`,`B`.`ip`,`B`.`ts`,`B`.`type`,`B`.`bad_product_number`,`B`.`device_state`,`B`.`good_product_number`,`B`.`mark` FROM src AS A CROSS JOIN UNNEST(A) AS B (`device_id`, `groupName`,`ip`,`ts`,`type`,`bad_product_number`,`device_state`,`good_product_number`,`mark`)");
        TableResult tableResult = tEnv.executeSql("SELECT * FROM src");

        // 打印输出结果
        tableResult.print();
        // 启动任务并等待任务完成
        tEnv.execute("Flink MQTT Source Example");
    }


}

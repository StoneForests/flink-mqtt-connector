package com.example.flink.connector.mqtt.table;

import org.apache.flink.table.api.*;

public class FlinkTableSourceSinkUsingTableDescriptorWithEmbeddedJson {

    public static void main(String[] args) {

        /*
        嵌套json如下
        {
            "et": "2022-12-19 13:49:20.123",
            "da": [
                {
                    "id": "xx1",
                    "da": {
                        "pointCode": "pp1",
                        "wendu": 1
                    }
                },
                {
                    "id": "xx2",
                    "da": {
                        "pointCode": "pp2",
                        "wendu": 2
                    }
                }
            ]
        }
        * */

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Schema sourceSchema = Schema.newBuilder()
                .column("et", "STRING")
                .column("da", "ARRAY<ROW<`id` STRING,`da` ROW<`pointCode` STRING,`wendu` int>>>")
                .build();
        TableDescriptor sourceDescriptor = TableDescriptor
                .forConnector("mqtt")
                .schema(sourceSchema)
                .format("json")
                .option("hostUrl", "tcp://10.0.113.61:1883")
                .option("username", "emqxadmin")
                .option("password", "JevDDzb!5Qfh5Fmr")
                .option("topics", "mqtt/source1,mqtt/source2")
                .option("clientIdPrefix", "source_client")
                .option("cleanSession", "true")
                .option("autoReconnect", "true")
                .option("connectionTimeout", "30")
                .option("keepAliveInterval", "60")
                .build();
        tEnv.createTemporaryTable("source", sourceDescriptor);

        String sinkSql = "create table t_out(id string,  pointCode STRING,  wendu INTEGER,  et string) with ('connector' = 'print')";
        tEnv.executeSql(sinkSql);

        //将flink_mqtt_source中的结果写入到flink_mqtt_sink表中，该语句会一直从mqtt中读取数据并写入到mqtt
        tEnv.executeSql("INSERT INTO\n" +
                "    t_out\n" +
                "SELECT\n" +
                "    `B`.`id`,\n" +
                "    `B`.`da`.`pointCode` as `pointCode`,\n" +
                "    `B`.`da`.`wendu` as `wendu`,\n" +
                "    `A`.`et`\n" +
                "FROM\n" +
                "    source as A\n" +
                "    CROSS JOIN UNNEST(`A`.`da`) AS B (`id`, `da`)");
    }


}

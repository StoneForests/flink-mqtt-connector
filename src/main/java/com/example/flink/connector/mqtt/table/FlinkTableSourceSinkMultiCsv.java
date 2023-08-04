package com.example.flink.connector.mqtt.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkTableSourceSinkMultiCsv {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        String sourceSql = "create table flink_mqtt_source(\n" +
                "  username STRING,\n" +
                "  age INTEGER\n" +
                ") with (\n" +
                "    'connector' = 'mqtt',\n" +
                "    'hostUrl' = 'tcp://10.0.113.61:1883',\n" +
                "    'username' = 'emqxadmin',\n" +
                "    'password' = 'JevDDzb!5Qfh5Fmr',\n" +
                "    'sourceTopics' = 'mqtt/source1,mqtt/source2',\n" +
                "    'clientIdPrefix' = 'source_client2',\n" +
                "    'cleanSession' = 'true',\n" +
                "    'autoReconnect' = 'true',\n" +
                "    'connectionTimeout' = '30',\n" +
                "    'keepAliveInterval' = '60',\n" +
                "    'format' = 'csv'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        String sinkSql = "create table flink_mqtt_sink(\n" +
                "  username STRING,\n" +
                "  age INTEGER\n" +
                ") with (\n" +
                "    'connector' = 'mqtt',\n" +
                "    'hostUrl' = 'tcp://10.0.113.61:1883',\n" +
                "    'username' = 'emqxadmin',\n" +
                "    'password' = 'JevDDzb!5Qfh5Fmr',\n" +
                "    'sinkTopics' = 'mqtt/sink1,mqtt/sink2',\n" +
                "    'qos' = '1',\n" +
                "    'clientIdPrefix' = 'sink_client2',\n" +
                "    'autoReconnect' = 'true',\n" +
                "    'connectionTimeout' = '30',\n" +
                "    'keepAliveInterval' = '60',\n" +
                "    'sink.parallelism' = '4',\n" +
                "    'format' = 'csv'\n" +
                ")";
        tEnv.executeSql(sinkSql);

        //将flink_mqtt_source中的结果写入到flink_mqtt_sink表中，该语句会一直从mqtt中读取数据并写入到mqtt
        tEnv.executeSql("INSERT INTO flink_mqtt_sink SELECT username,age FROM flink_mqtt_source");
        //下面两句与上面的insert into效果是一样的
//        Table sourceTable = tEnv.from("flink_mqtt_source");
//        sourceTable.executeInsert("flink_mqtt_sink");
    }


}

package com.example.flink.connector.mqtt.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class FlinkTableJustSource {

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
                "    'clientIdPrefix' = 'source_client1',\n" +
                "    'cleanSession' = 'true',\n" +
                "    'autoReconnect' = 'true',\n" +
                "    'connectionTimeout' = '30',\n" +
                "    'keepAliveInterval' = '60',\n" +
                "    'format' = 'json'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        //查询flink_mqtt_source中的数据，下述语句会阻塞住，
        TableResult tableResult = tEnv.executeSql("SELECT * FROM flink_mqtt_source");
        // 打印输出结果
        tableResult.print();
        // 启动任务并等待任务完成
        tEnv.execute("Flink MQTT Source Example");
    }

}

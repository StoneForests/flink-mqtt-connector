package com.example.flink.connector.mqtt.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkTableJustSourceUsingStreamTable {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String sourceSql = "create table flink_mqtt_source(\n" +
                "  username STRING,\n" +
                "  age INTEGER\n" +
                ") with (\n" +
                "    'connector' = 'mqtt',\n" +
                "    'hostUrl' = 'tcp://10.0.113.61:1883',\n" +
                "    'username' = 'emqxadmin',\n" +
                "    'password' = 'JevDDzb!5Qfh5Fmr',\n" +
                "    'topics' = 'mqtt/source1,mqtt/source2',\n" +
                "    'clientIdPrefix' = 'source_client1',\n" +
                "    'cleanSession' = 'true',\n" +
                "    'autoReconnect' = 'true',\n" +
                "    'connectionTimeout' = '30',\n" +
                "    'keepAliveInterval' = '60',\n" +
                "    'format' = 'json'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        //查询flink_mqtt_source中的数据，下述语句会阻塞住，
        Table table = tEnv.sqlQuery("SELECT * FROM flink_mqtt_source");
        // 将结果转换为 DataStream
        DataStream<Row> resultStream = tEnv.toDataStream(table);
        // 打印输出结果
        resultStream.print();
        // 执行作业
        env.execute("Table to DataStream Example");
    }

}

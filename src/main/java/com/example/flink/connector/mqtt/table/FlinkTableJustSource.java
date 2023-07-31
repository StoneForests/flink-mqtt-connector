package com.example.flink.connector.mqtt.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

public class FlinkTableJustSource {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        String sourceSql = "create table flink_mqtt_source(\n" +
                "  username STRING,\n" +
                "  age INTEGER\n" +
                ") with (\n" +
                "    'connector' = 'mqtt',\n" +
                "    'hosturl' = 'tcp://192.168.1.1:1883',\n" +
                "    'username' = 'abcabc',\n" +
                "    'password' = '123123',\n" +
                "    'topic' = 'mqtt/wordCount',\n" +
                "    'format' = 'json'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        //查询flink_mqtt_source中的数据，下述语句会阻塞住，
        tEnv.executeSql("SELECT * FROM flink_mqtt_source").print();

        // 启动任务并等待任务完成
        tEnv.execute("Flink MQTT Source Example");
    }

}

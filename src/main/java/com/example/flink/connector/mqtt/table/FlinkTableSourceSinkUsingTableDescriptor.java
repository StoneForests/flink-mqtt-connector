package com.example.flink.connector.mqtt.table;

import org.apache.flink.table.api.*;

public class FlinkTableSourceSinkUsingTableDescriptor {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Schema sourceSchema = Schema.newBuilder()
                .column("username", "STRING")
                .column("age", "INTEGER")
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
        tEnv.createTemporaryTable("flink_mqtt_source", sourceDescriptor);

        Schema sinkSchema = Schema.newBuilder()
                .column("username", "STRING")
                .column("age", "INTEGER")
                .build();
        TableDescriptor sinkDescriptor = TableDescriptor
                .forConnector("mqtt")
                .schema(sinkSchema)
                .format("json")
                .option("hostUrl", "tcp://10.0.113.61:1883")
                .option("username", "emqxadmin")
                .option("password", "JevDDzb!5Qfh5Fmr")
                .option("topics", "mqtt/sink1,mqtt/sink2")
                .option("qos", "1")
                .option("clientIdPrefix", "sink_client")
                .option("autoReconnect", "true")
                .option("connectionTimeout", "30")
                .option("keepAliveInterval", "60")
//                .option("sink.parallelism", "4")
                .build();
        tEnv.createTemporaryTable("flink_mqtt_sink", sinkDescriptor);

        //将flink_mqtt_source中的结果写入到flink_mqtt_sink表中，该语句会一直从mqtt中读取数据并写入到mqtt
//        tEnv.executeSql("INSERT INTO flink_mqtt_sink SELECT username,age FROM flink_mqtt_source");
        //下面两句与上面的insert into效果是一样的，但他限制了必须是insert语句
//        tEnv.createStatementSet().addInsertSql("INSERT INTO flink_mqtt_sink SELECT username,age FROM flink_mqtt_source").execute();
        //下面两句与上面的insert into效果是一样的，但他不需要写sql，而且是全字段匹配
        Table sourceTable = tEnv.from(sourceDescriptor);
        sourceTable.executeInsert(sinkDescriptor);
    }


}

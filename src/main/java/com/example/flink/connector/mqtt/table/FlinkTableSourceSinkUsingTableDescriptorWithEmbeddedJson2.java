package com.example.flink.connector.mqtt.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkTableSourceSinkUsingTableDescriptorWithEmbeddedJson2 {

    public static void main(String[] args) {

        /*
        嵌套json如下
        {
            "dt":"2023-09-01 00:00:00",
            "str":{
                "name":"wj",
                "xuexi":["yuwen","shuxue"]
            }
        }
        * */

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Schema sourceSchema = Schema.newBuilder()
                .column("dt", "STRING")
                .column("str", "ROW<`name` STRING,`xuexi` ARRAY<STRING>>")
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

        String sinkSql = "create table t_out(name string,  xuexi STRING, dt string) with ('connector' = 'print')";
        tEnv.executeSql(sinkSql);

        //将flink_mqtt_source中的结果写入到flink_mqtt_sink表中
        tEnv.executeSql("INSERT INTO\n" +
                "    t_out\n" +
                "SELECT\n" +
                "    `A`.`str`.`name`,\n" +
                "    `B`.`xuexi` as `xuexi`,\n" +
                "    `A`.`dt`\n" +
                "FROM\n" +
                "    source as A\n" +
                "    CROSS JOIN UNNEST(`A`.`str`.`xuexi`) AS B (`xuexi`)");
    }


}

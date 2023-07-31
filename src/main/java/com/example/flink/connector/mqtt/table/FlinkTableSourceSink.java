package com.example.flink.connector.mqtt.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkTableSourceSink {

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

        String sinkSql = "create table flink_mqtt_sink(\n" +
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
        tEnv.executeSql(sinkSql);

        //将flink_mqtt_source中的结果写入到flink_mqtt_sink表中，这里有个bug，flink根本就没来得及写入数据，就断开了mqtt侧的连接，之后死循环地重连，chatgpt的回复如下
        // 在使用 Table API 的情况下，executeSql 方法执行 INSERT INTO 语句会立即执行该语句并完成写入操作，然后结束作业。因此，在这个示例中，tEnv.executeSql(insertSql); 执行后，确实会将 flink_mqtt_source 的数据写入到 flink_mqtt_sink 表中，但也导致了 flink_mqtt_source 表监听的断开。
        // 由于 Flink SQL 是静态的，不适合用于实时的数据流，所以 Table API 在这种场景下使用比较受限。
        // 如果你希望在持续监听 MQTT 消息并实时写入 flink_mqtt_sink 表中，可以继续使用 DataStream API 的方式。上面之前给出的基于 DataStream API 的示例代码可以满足这个需求，因为 DataStream API 允许你使用异步或连续的方式处理实时数据流，保持 Flink 作业一直运行。
        // 所以，如果你希望保持持续监听 MQTT 消息并实时写入 flink_mqtt_sink 表中，建议使用 DataStream API 来处理。如果有其他特殊需求，也可以考虑使用 Flink 的 ProcessFunction 或其他更灵活的方式来实现。
        tEnv.executeSql("INSERT INTO flink_mqtt_sink SELECT username,age FROM flink_mqtt_source");
        //下面两句跟上面的insert into的效果是一样的
//        Table sourceTable = tEnv.from("flink_mqtt_source");
//        sourceTable.executeInsert("flink_mqtt_sink");
        // 触发作业执行
        tEnv.execute("Flink Table Job");
    }


}

package com.example.flink.connector.mqtt.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MqttWordCount2MqttPaho {


    public static void main(String[] args) throws Exception {
        /**
         * 监听mqtt topicIn的消息，统计每个单词出现的次数后将结果发送给mqtt  topicOut
         */
        String broker = "tcp://192.168.1.1:1883"; //mqtt的broker地址
        String username = "abcabc";  //mqtt的账号
        String password = "123123";  //mqtt的密码
        String topicIn = "mqtt/wordCount";   //监听的mqtt topic
        String topicOut = "mqtt/wordCount";  //发送的mqtt topic
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get input data
        DataStream<MyMqttMessage> text = env.addSource(new MqttSourcePaho(broker, username, password, topicIn, 0));

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter()).keyBy((KeySelector<Tuple2<String, Integer>, Object>) stringIntegerTuple2 -> stringIntegerTuple2.f0).sum(1);
        counts.print();
        counts.addSink(new MqttSinkPaho<>(broker, username, password, topicOut));

        // execute program
        env.execute("Flink MQTT Stream WordCount Example");
    }

    //
    // User Functions
    //

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and splits
     * it into multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    @SuppressWarnings("serial")
    public static final class LineSplitter implements FlatMapFunction<MyMqttMessage, Tuple2<String, Integer>> {

        @Override
        public void flatMap(MyMqttMessage value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.getPayload().toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
